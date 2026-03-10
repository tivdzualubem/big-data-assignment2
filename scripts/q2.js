db = db.getSiblingDB("ecommerce");

const targetUserId = NumberLong("557442625");

print("Top personalized recommended products for target user:", targetUserId);

const seenProducts = db.events.distinct("product_id", { user_id: targetUserId });

const friendIds = db.friends.aggregate([
  {
    $match: {
      $or: [
        { friend1: targetUserId },
        { friend2: targetUserId }
      ]
    }
  },
  {
    $project: {
      _id: 0,
      friend_id: {
        $cond: [
          { $eq: ["$friend1", targetUserId] },
          "$friend2",
          "$friend1"
        ]
      }
    }
  }
]).toArray().map(doc => doc.friend_id);

db.events.aggregate([
  {
    $match: {
      user_id: { $in: friendIds },
      product_id: { $nin: seenProducts }
    }
  },
  {
    $addFields: {
      score: {
        $switch: {
          branches: [
            { case: { $eq: ["$event_type", "purchase"] }, then: 5 },
            { case: { $eq: ["$event_type", "cart"] }, then: 3 },
            { case: { $eq: ["$event_type", "view"] }, then: 1 }
          ],
          default: 0
        }
      }
    }
  },
  {
    $group: {
      _id: "$product_id",
      recommendation_score: { $sum: "$score" },
      category_code: { $last: "$category_code" },
      brand: { $last: "$brand" },
      price: { $last: "$price" }
    }
  },
  {
    $project: {
      _id: 0,
      product_id: "$_id",
      category_code: 1,
      brand: 1,
      price: 1,
      recommendation_score: 1
    }
  },
  { $sort: { recommendation_score: -1, product_id: 1 } },
  { $limit: 10 }
]).forEach(printjson);
