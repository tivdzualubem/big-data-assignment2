db = db.getSiblingDB("ecommerce");

print("Retrieving products from the recommended product list");

db.events.find(
  {
    product_id: { $in: [28718136, 12709709, 12710984, 12711508, 12720525] }
  },
  {
    _id: 0,
    product_id: 1,
    event_type: 1,
    price: 1
  }
).limit(10).forEach(doc => printjson(doc));
