package main

import (
	"fmt"

	"github.com/filecoin-project/boost/extern/boostd-data/ldb"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/extern/boostd-data/svc"
	"github.com/filecoin-project/boost/extern/boostd-data/yugabyte"
	"github.com/filecoin-project/go-address"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var migrateCmd = &cli.Command{
	Name:        "migrate",
	Description: "Migrate boost piece information and indices from leveldb to yugabyte",
	Usage:       "LtoY migrate",
	Before:      before,
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name:     "hosts",
			Usage:    "yugabyte hosts to connect to over cassandra interface eg '127.0.0.1'",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "connect-string",
			Usage:    "postgres connect string eg 'postgresql://postgres:postgres@localhost'",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "insert-parallelism",
			Usage: "the number of threads to use when inserting into the PayloadToPieces index",
			Value: 16,
		},
		&cli.IntFlag{
			Name:     "CQLTimeout",
			Usage:    "client timeout value in seconds for CQL queries",
			Required: false,
			Value:    yugabyte.CqlTimeout,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		if cctx.Args().Len() == 0 {
			return fmt.Errorf("must specify either dagstore or pieceinfo migration")
		}
		repoDir, err := homedir.Expand(cctx.String(FlagBoostRepo))
		if err != nil {
			return err
		}

		repoPath, err := svc.MakeLevelDBDir(repoDir)
		if err != nil {
			return err
		}

		// Create a connection to the leveldb store
		lstore := ldb.NewStore(repoPath)
		err = lstore.Start(ctx)
		if err != nil {
			return err
		}

		// Create a connection to the yugabyte local index directory
		settings := yugabyte.DBSettings{
			Hosts:                    cctx.StringSlice("hosts"),
			ConnectString:            cctx.String("connect-string"),
			PayloadPiecesParallelism: cctx.Int("insert-parallelism"),
			CQLTimeout:               cctx.Int("CQLTimeout"),
		}

		// Note that it doesn't matter what address we pass here: because the
		// table is newly created, it doesn't contain any rows when the
		// migration is run.
		migrator := yugabyte.NewMigrator(settings, address.TestAddress)
		ystore := yugabyte.NewStore(settings, migrator)
		err = ystore.Start(ctx)
		if err != nil {
			return err
		}

		//List all pieces from leveldb
		pieces, err := lstore.ListPieces(ctx)
		if err != nil {
			return err
		}

		for _, piece := range pieces {
			//first migrate index
			//Get index from leveldb
			resp, err := lstore.GetIndex(ctx, piece)
			if err != nil {
				return err
			}
			var records []model.Record
			for r := range resp {
				if r.Error != nil {
					return r.Error
				}
				records = append(records, r.Record)
			}

			//Add index to yugabytedb
			respch := ystore.AddIndex(ctx, piece, records, false)
			for resp := range respch {
				if resp.Err != "" {
					return fmt.Errorf("adding index %s to store: %s", piece, resp.Err)
				}
			}

			log.Debugw("migrate index", "piece", piece, "records", records)

			//second migrate piece info
			deals, err := lstore.GetPieceDeals(ctx, piece)
			if err != nil {
				return err
			}

			for _, deal := range deals {
				err := ystore.AddDealForPiece(ctx, piece, deal)
				if err != nil {
					return err
				}
			}

			log.Debugw("migrate piece info", "piece", piece, "deals", deals)

		}

		return nil
	},
}
