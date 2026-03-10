db = db.getSiblingDB("ecommerce")

const result = db.messages.aggregate([
  {
    $group: {
      _id: "$campaign_id",
      total_messages: { $sum: 1 },
      purchases: {
        $sum: {
          $cond: [
            { $eq: ["$is_purchased", true] },
            1,
            0
          ]
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      campaign_id: "$_id",
      total_messages: 1,
      purchases: 1,
      purchase_rate_percent: {
        $round: [
          {
            $multiply: [
              { $divide: ["$purchases", "$total_messages"] },
              100
            ]
          },
          2
        ]
      }
    }
  },
  {
    $sort: { purchase_rate_percent: -1 }
  },
  {
    $limit: 10
  }
])

print("Top campaigns by purchase rate:")
result.forEach(doc => printjson(doc))
