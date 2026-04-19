use NosCites
db.listing_paris.aggregate([
  { $match: { host_id: { $exists: true, $ne: null, $ne: "" }, host_is_superhost: "t" } },
  { $group: { _id: "$host_id" } },
  { $count: "nb_hotes_superhost" }
]);
db.listing_paris.aggregate([
  { $match: { host_id: { $exists: true, $ne: null, $ne: "" } } },

  { $group: {
      _id: "$host_id",
      is_superhost: { $max: { $cond: [ { $eq: [ "$host_is_superhost", "t" ] }, 1, 0 ] } }
  }},

  { $group: {
      _id: null,
      total_hotes: { $sum: 1 },
      hotes_superhost: { $sum: "$is_superhost" }
  }},

  { $project: {
      _id: 0,
      total_hotes: 1,
      hotes_superhost: 1,
      pourcentage_superhost: {
        $cond: [
          { $gt: [ "$total_hotes", 0 ] },
          { $round: [ { $multiply: [ { $divide: [ "$hotes_superhost", "$total_hotes" ] }, 100 ] }, 2 ] },
          0
        ]
      }
  }}
]);
