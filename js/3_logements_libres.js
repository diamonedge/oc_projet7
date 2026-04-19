use NosCites
db.listing_paris.aggregate( [
   { $match: { has_availability: "t" } },
   {
         $group: {
            _id: "Logements avec dispo",
            count: { $sum: 1 }
         }
   },
   { $sort: { total: -1 } }
] );
