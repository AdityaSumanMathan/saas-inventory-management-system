const express = require('express');
const { body, query, validationResult } = require('express-validator');
const db = require('../database');
const { authMiddleware, authorize } = require('../authMiddleware');

const router = express.Router();

// Helper function to generate purchase order number
async function generateOrderNumber(organizationId) {
  const year = new Date().getFullYear();
  const count = await db('purchase_orders')
    .where('organization_id', organizationId)
    .whereRaw('YEAR(created_at) = ?', [year])
    .count('* as count')
    .first();
  
  const orderNumber = `PO-${year}-${String(parseInt(count.count) + 1).padStart(4, '0')}`;
  return orderNumber;
}

// Get all purchase orders
router.get('/', authMiddleware, [
  query('page').optional().isInt({ min: 1 }).withMessage('Page must be positive integer'),
  query('limit').optional().isInt({ min: 1, max: 100 }).withMessage('Limit must be 1-100'),
  query('supplier_id').optional().isUUID().withMessage('Supplier ID must be valid UUID'),
  query('status').optional().isIn(['draft', 'sent', 'confirmed', 'partially_received', 'received', 'cancelled']).withMessage('Invalid status'),
  query('date_from').optional().isISO8601().withMessage('Date from must be valid ISO date'),
  query('date_to').optional().isISO8601().withMessage('Date to must be valid ISO date')
], async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ 
        error: 'Validation failed', 
        details: errors.array() 
      });
    }

    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;
    const { supplier_id, status, date_from, date_to } = req.query;

    let query = db('purchase_orders as po')
      .select(
        'po.id',
        'po.order_number',
        'po.order_date',
        'po.status',
        'po.total_amount',
        'po.notes',
        'po.expected_delivery_date',
        'po.created_at',
        's.name as supplier_name',
        's.contact_person',
        's.email as supplier_email',
        db.raw('COUNT(DISTINCT poi.id) as item_count')
      )
      .join('suppliers as s', 'po.supplier_id', '=', 's.id')
      .leftJoin('purchase_order_items as poi', 'po.id', '=', 'poi.purchase_order_id')
      .where('po.organization_id', req.user.organization_id);

    // Apply filters
    if (supplier_id) {
      query = query.where('po.supplier_id', supplier_id);
    }

    if (status) {
      query = query.where('po.status', status);
    }

    if (date_from) {
      query = query.where('po.order_date', '>=', new Date(date_from));
    }

    if (date_to) {
      query = query.where('po.order_date', '<=', new Date(date_to));
    }

    // Get total count
    const countQuery = query.clone().count('DISTINCT po.id as total');
    const [{ total }] = await countQuery;
    const totalOrders = parseInt(total);

    // Apply pagination and ordering
    query = query
      .groupBy('po.id', 's.id')
      .orderBy('po.created_at', 'desc')
      .limit(limit)
      .offset(offset);

    const orders = await query;

    // Calculate pagination metadata
    const totalPages = Math.ceil(totalOrders / limit);

    res.json({
      data: {
        purchase_orders: orders,
        pagination: {
          current_page: page,
          per_page: limit,
          total: totalOrders,
          total_pages: totalPages,
          has_next_page: page < totalPages,
          has_prev_page: page > 1
        }
      }
    });

  } catch (error) {
    console.error('Get purchase orders error:', error);
    res.status(500).json({ 
      error: 'Failed to fetch purchase orders', 
      message: error.message 
    });
  }
});

// Get single purchase order by ID
router.get('/:id', authMiddleware, async (req, res) => {
  try {
    const { id } = req.params;

    const order = await db('purchase_orders as po')
      .select(
        'po.*',
        's.name as supplier_name',
        's.contact_person',
        's.email as supplier_email',
        's.phone as supplier_phone',
        's.address as supplier_address',
        's.payment_terms_days'
      )
      .join('suppliers as s', 'po.supplier_id', '=', 's.id')
      .where('po.id', id)
      .where('po.organization_id', req.user.organization_id)
      .first();

    if (!order) {
      return res.status(404).json({ 
        error: 'Purchase order not found' 
      });
    }

    // Get purchase order items
    const items = await db('purchase_order_items as poi')
      .select(
        'poi.*',
        'p.sku',
        'p.name as product_name',
        'p.description as product_description',
        'p.unit_of_measure',
        db.raw('COALESCE(received_summary.received_quantity, 0) as received_quantity')
      )
      .join('products as p', 'poi.product_id', '=', 'p.id')
      .leftJoin(
        db.raw(`
          (SELECT 
            purchase_order_item_id,
            SUM(quantity) as received_quantity
          FROM purchase_receipts 
          GROUP BY purchase_order_item_id) as received_summary
        `),
        'poi.id', '=', 'received_summary.purchase_order_item_id'
      )
      .where('poi.purchase_order_id', id);

    // Calculate summary
    const summary = items.reduce((acc, item) => {
      acc.total_items += 1;
      acc.total_quantity += parseFloat(item.quantity);
      acc.total_amount += parseFloat(item.total_amount);
      return acc;
    }, { total_items: 0, total_quantity: 0, total_amount: 0 });

    res.json({
      data: {
        purchase_order: {
          ...order,
          items,
          summary
        }
      }
    });

  } catch (error) {
    console.error('Get purchase order error:', error);
    res.status(500).json({ 
      error: 'Failed to fetch purchase order', 
      message: error.message 
    });
  }
});

// Create new purchase order
router.post('/', authMiddleware, [
  body('supplier_id').isUUID().withMessage('Supplier ID must be valid UUID'),
  body('items').isArray({ min: 1 }).withMessage('At least one item is required'),
  body('items.*.product_id').isUUID().withMessage('Product ID must be valid UUID'),
  body('items.*.quantity').isFloat({ min: 0.01 }).withMessage('Quantity must be positive'),
  body('items.*.unit_price').isFloat({ min: 0 }).withMessage('Unit price must be non-negative'),
  body('expected_delivery_date').optional().isISO8601().withMessage('Expected delivery date must be valid'),
  body('notes').optional().isLength({ max: 1000 }).withMessage('Notes too long')
], async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ 
        error: 'Validation failed', 
        details: errors.array() 
      });
    }

    const {
      supplier_id,
      items,
      expected_delivery_date,
      notes
    } = req.body;

    // Verify supplier exists and belongs to user's organization
    const supplier = await db('suppliers')
      .where('id', supplier_id)
      .where('organization_id', req.user.organization_id)
      .where('is_active', true)
      .first();

    if (!supplier) {
      return res.status(404).json({ 
        error: 'Supplier not found or inactive' 
      });
    }

    // Verify all products exist and belong to user's organization
    for (const item of items) {
      const product = await db('products')
        .where('id', item.product_id)
        .where('organization_id', req.user.organization_id)
        .where('is_active', true)
        .first();

      if (!product) {
        return res.status(400).json({ 
          error: `Product not found: ${item.product_id}` 
        });
      }
    }

    // Generate order number
    const orderNumber = await generateOrderNumber(req.user.organization_id);

    // Create purchase order in transaction
    const result = await db.transaction(async trx => {
      // Create purchase order
      const [purchaseOrder] = await trx('purchase_orders')
        .insert({
          organization_id: req.user.organization_id,
          supplier_id,
          order_number: orderNumber,
          order_date: new Date(),
          status: 'draft',
          total_amount: 0, // Will be calculated
          expected_delivery_date,
          notes
        })
        .returning('*');

      // Create purchase order items
      let totalAmount = 0;
      const orderItems = [];

      for (const item of items) {
        const totalItemAmount = parseFloat(item.quantity) * parseFloat(item.unit_price);
        totalAmount += totalItemAmount;

        const [orderItem] = await trx('purchase_order_items')
          .insert({
            purchase_order_id: purchaseOrder[0].id,
            product_id: item.product_id,
            quantity: parseFloat(item.quantity),
            unit_price: parseFloat(item.unit_price),
            total_amount: totalItemAmount
          })
          .returning('*');

        orderItems.push(orderItem);
      }

      // Update total amount
      await trx('purchase_orders')
        .where('id', purchaseOrder[0].id)
        .update({ total_amount: totalAmount });

      return {
        purchase_order: { ...purchaseOrder[0], total_amount: totalAmount },
        items: orderItems
      };
    });

    res.status(201).json({
      message: 'Purchase order created successfully',
      data: result
    });

  } catch (error) {
    console.error('Create purchase order error:', error);
    res.status(500).json({ 
      error: 'Failed to create purchase order', 
      message: error.message 
    });
  }
});

// Update purchase order status
router.patch('/:id/status', authMiddleware, [
  body('status').isIn(['sent', 'confirmed', 'partially_received', 'received', 'cancelled']).withMessage('Invalid status')
], async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ 
        error: 'Validation failed', 
        details: errors.array() 
      });
    }

    const { id } = req.params;
    const { status } = req.body;

    // Verify purchase order exists and belongs to user's organization
    const order = await db('purchase_orders')
      .where('id', id)
      .where('organization_id', req.user.organization_id)
      .first();

    if (!order) {
      return res.status(404).json({ 
        error: 'Purchase order not found' 
      });
    }

    // Validate status transition
    const validTransitions = {
      'draft': ['sent'],
      'sent': ['confirmed', 'cancelled'],
      'confirmed': ['partially_received', 'received', 'cancelled'],
      'partially_received': ['received', 'cancelled'],
      'received': [],
      'cancelled': []
    };

    if (!validTransitions[order.status].includes(status)) {
      return res.status(400).json({ 
        error: `Cannot change status from ${order.status} to ${status}` 
      });
    }

    await db('purchase_orders')
      .where('id', id)
      .update({ 
        status,
        updated_at: new Date()
      });

    const updatedOrder = await db('purchase_orders')
      .where('id', id)
      .first();

    res.json({
      message: 'Purchase order status updated successfully',
      data: { purchase_order: updatedOrder }
    });

  } catch (error) {
    console.error('Update purchase order status error:', error);
    res.status(500).json({ 
      error: 'Failed to update purchase order status', 
      message: error.message 
    });
  }
});

// Record goods receipt (receive items)
router.post('/:id/receive', authMiddleware, [
  body('items').isArray({ min: 1 }).withMessage('At least one item is required'),
  body('items.*.purchase_order_item_id').isUUID().withMessage('Purchase order item ID must be valid UUID'),
  body('items.*.received_quantity').isFloat({ min: 0.01 }).withMessage('Received quantity must be positive'),
  body('items.*.received_date').optional().isISO8601().withMessage('Received date must be valid'),
  body('notes').optional().isLength({ max: 500 }).withMessage('Notes too long')
], async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ 
        error: 'Validation failed', 
        details: errors.array() 
      });
    }

    const { id } = req.params;
    const { items, notes } = req.body;

    // Verify purchase order exists and belongs to user's organization
    const order = await db('purchase_orders')
      .where('id', id)
      .where('organization_id', req.user.organization_id)
      .whereIn('status', ['confirmed', 'partially_received'])
      .first();

    if (!order) {
      return res.status(404).json({ 
        error: 'Purchase order not found or cannot receive items' 
      });
    }

    // Process receipts in transaction
    const result = await db.transaction(async trx => {
      const receipts = [];
      let totalReceivedValue = 0;

      for (const item of items) {
        const { purchase_order_item_id, received_quantity, received_date } = item;

        // Get purchase order item
        const orderItem = await trx('purchase_order_items')
          .where('id', purchase_order_item_id)
          .where('purchase_order_id', id)
          .first();

        if (!orderItem) {
          throw new Error(`Purchase order item not found: ${purchase_order_item_id}`);
        }

        // Check if received quantity exceeds ordered quantity
        const previouslyReceived = await trx('purchase_receipts')
          .where('purchase_order_item_id', purchase_order_item_id)
          .sum('quantity as total_received')
          .first();

        const totalReceived = parseFloat(previouslyReceived.total_received) || 0;
        const maxReceivable = parseFloat(orderItem.quantity) - totalReceived;

        if (received_quantity > maxReceivable) {
          throw new Error(`Received quantity (${received_quantity}) exceeds remaining quantity (${maxReceivable}) for item ${purchase_order_item_id}`);
        }

        // Create receipt record
        const [receipt] = await trx('purchase_receipts')
          .insert({
            organization_id: req.user.organization_id,
            purchase_order_id: id,
            purchase_order_item_id,
            user_id: req.user.id,
            quantity: parseFloat(received_quantity),
            unit_price: orderItem.unit_price,
            total_amount: parseFloat(received_quantity) * parseFloat(orderItem.unit_price),
            received_date: received_date || new Date(),
            notes
          })
          .returning('*');

        receipts.push(receipt);
        totalReceivedValue += receipt.total_amount;

        // Create inventory transaction for received items
        const currentStock = await trx('inventory_transactions')
          .where('product_id', orderItem.product_id)
          .where('organization_id', req.user.organization_id)
          .sum('quantity as total_stock')
          .first();

        const previousStock = parseFloat(currentStock.total_stock) || 0;
        const newStock = previousStock + parseFloat(received_quantity);

        await trx('inventory_transactions')
          .insert({
            organization_id: req.user.organization_id,
            product_id: orderItem.product_id,
            user_id: req.user.id,
            quantity: parseFloat(received_quantity),
            previous_stock: previousStock,
            new_stock: newStock,
            transaction_type: 'purchase',
            reference_number: order.order_number,
            notes: `Received via PO ${order.order_number}`
          });
      }

      // Update purchase order status
      const allItems = await trx('purchase_order_items')
        .where('purchase_order_id', id);

      let fullyReceived = true;
      let partiallyReceived = false;

      for (const item of allItems) {
        const received = await trx('purchase_receipts')
          .where('purchase_order_item_id', item.id)
          .sum('quantity as total_received')
          .first();

        const totalReceived = parseFloat(received.total_received) || 0;
        if (totalReceived === 0) {
          fullyReceived = false;
        } else if (totalReceived < parseFloat(item.quantity)) {
          partiallyReceived = true;
          fullyReceived = false;
        }
      }

      let newStatus = order.status;
      if (fullyReceived) {
        newStatus = 'received';
      } else if (partiallyReceived) {
        newStatus = 'partially_received';
      }

      await trx('purchase_orders')
        .where('id', id)
        .update({ 
          status: newStatus,
          updated_at: new Date()
        });

      return {
        receipts,
        new_status: newStatus,
        total_received_value: totalReceivedValue
      };
    });

    res.json({
      message: 'Goods received successfully',
      data: result
    });

  } catch (error) {
    console.error('Record receipt error:', error);
    res.status(500).json({ 
      error: 'Failed to record receipt', 
      message: error.message 
    });
  }
});

// Delete purchase order (draft only)
router.delete('/:id', authMiddleware, authorize('admin', 'manager'), async (req, res) => {
  try {
    const { id } = req.params;

    const order = await db('purchase_orders')
      .where('id', id)
      .where('organization_id', req.user.organization_id)
      .first();

    if (!order) {
      return res.status(404).json({ 
        error: 'Purchase order not found' 
      });
    }

    if (order.status !== 'draft') {
      return res.status(400).json({ 
        error: 'Cannot delete purchase order that has been sent or received' 
      });
    }

    // Check if there are any receipts
    const receiptCount = await db('purchase_receipts')
      .where('purchase_order_id', id)
      .count('* as count')
      .first();

    if (parseInt(receiptCount.count) > 0) {
      return res.status(400).json({ 
        error: 'Cannot delete purchase order with existing receipts' 
      });
    }

    // Delete in transaction
    await db.transaction(async trx => {
      // Delete purchase order items
      await trx('purchase_order_items')
        .where('purchase_order_id', id)
        .del();

      // Delete purchase order
      await trx('purchase_orders')
        .where('id', id)
        .del();
    });

    res.json({
      message: 'Purchase order deleted successfully',
      data: { deleted: true }
    });

  } catch (error) {
    console.error('Delete purchase order error:', error);
    res.status(500).json({ 
      error: 'Failed to delete purchase order', 
      message: error.message 
    });
  }
});

module.exports = router;