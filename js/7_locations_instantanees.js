use NosCites
db.listing_paris.aggregate([
  {
    $addFields: {
      instant_norm: {
        $toLower: {
          $convert: {
            input: "$instant_bookable",
            to: "string",
            onError: "",
            onNull: ""
          }
        }
      }
    }
  },
  {
    $group: {
      _id: null,
      total_annonces: { $sum: 1 },
      instant_bookable_annonces: {
        $sum: {
          $cond: [
            { $in: ["$instant_norm", ["t", "true", "1", "yes", "y"]] },
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
      total_annonces: 1,
      instant_bookable_annonces: 1,
      pourcentage_instant_bookable: {
        $cond: [
          { $gt: ["$total_annonces", 0] },
          { $round: [{ $multiply: [{ $divide: ["$instant_bookable_annonces", "$total_annonces"] }, 100] }, 2] },
          0
        ]
      }
    }
  }
]);
