use NosCites
db.listing_paris.aggregate([
  {
    $match: {
      host_id: { $exists: true, $ne: null, $ne: "" }
    }
  },

  // 1) Comptage du nombre d'annonces par hôte (host_id)
  {
    $group: {
      _id: "$host_id",
      host_name: { $first: "$host_name" },
      host_url: { $first: "$host_url" },
      annonces_dans_collection: { $sum: 1 }
    }
  },

  // 2) Deux sorties dans une seule requête : liste + stats
  {
    $facet: {
      // Liste des hôtes > 100 annonces (tri décroissant)
      hotes_plus_100: [
        { $match: { annonces_dans_collection: { $gt: 100 } } },
        { $sort: { annonces_dans_collection: -1 } },
        {
          $project: {
            _id: 0,
            host_id: "$_id",
            host_name: 1,
            host_url: 1,
            annonces_dans_collection: 1
          }
        }
      ],

      // Statistiques globales
      stats: [
        {
          $group: {
            _id: null,
            total_hotes: { $sum: 1 },
            hotes_plus_100: {
              $sum: {
                $cond: [{ $gt: ["$annonces_dans_collection", 100] }, 1, 0]
              }
            }
          }
        },
        {
          $project: {
            _id: 0,
            total_hotes: 1,
            hotes_plus_100: 1,
            pourcentage_hotes_plus_100: {
              $round: [
                { $multiply: [{ $divide: ["$hotes_plus_100", "$total_hotes"] }, 100] },
                2
              ]
            }
          }
        }
      ]
    }
  }
]);
