use NosCites
db.listing_paris.aggregate([
  {
    $addFields: {
      number_of_reviews_int: {
        $convert: {
          input: "$number_of_reviews",
          to: "int",
          onError: 0,
          onNull: 0
        }
      }
    }
  },
  { $sort: { number_of_reviews_int: -1 } },
  { $limit: 5 },
  {
    $project: {
      _id: 1,
      id: 1,
      name: 1,
      listing_url: 1,
      number_of_reviews: 1,
      number_of_reviews_int: 1
    }
  }
]);
