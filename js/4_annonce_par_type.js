use NosCites
db.listing_paris.aggregate( [
	{ $match: {  } },
   { $group: { _id: "$property_type",
            count: { $sum: 1 } } },
   { $sort: { total: -1 } }
] );
