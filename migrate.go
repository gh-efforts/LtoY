package main

import (
	"context"
	"fmt"

	"github.com/filecoin-project/boost/extern/boostd-data/ldb"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/extern/boostd-data/svc"
	"github.com/filecoin-project/boost/extern/boostd-data/yugabyte"
	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var migrateCmd = &cli.Command{
	Name:        "migrate",
	Description: "Migrate boost piece information and indices from leveldb to yugabyte",
	Usage:       "LtoY migrate [index/deal]",
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
		&cli.BoolFlag{
			Name:  "single",
			Usage: "migrate single index for test",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := cctx.Context

		var pieceCID cid.Cid
		var err error
		migrateType := cctx.Args().Get(0)
		if migrateType != "index" && migrateType != "deal" {
			pieceCID, err = cid.Parse(migrateType)
			if err != nil {
				return err
			}
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

		if migrateType == "index" {
			return migrateIndex(ctx, lstore, ystore, cctx.Bool("single"))
		} else if migrateType == "deal" {
			return migrateDeal(ctx, lstore, ystore)
		} else {
			return migrateCheck(ctx, lstore, ystore, pieceCID)
		}
	},
}

func migrateIndex(ctx context.Context, lstore *ldb.Store, ystore *yugabyte.Store, single bool) error {
	//List all pieces from leveldb
	pieces, err := lstore.ListPieces(ctx)
	if err != nil {
		return err
	}
	log.Infof("piece count in leveldb: %d", len(pieces))

	for _, piece := range pieces {
		isIndexed, err := ystore.IsIndexed(ctx, piece)
		if err != nil {
			return err
		}
		if isIndexed {
			log.Debugf("piece: %s alreat indexed", piece)
			continue
		}

		isCompleteIndex, err := lstore.IsCompleteIndex(ctx, piece)
		if err != nil {
			return err
		}

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
		respch := ystore.AddIndex(ctx, piece, records, isCompleteIndex)
		for resp := range respch {
			if resp.Err != "" {
				return fmt.Errorf("adding index %s to store: %s", piece, resp.Err)
			}
		}

		log.Debugw("migrate index", "piece", piece, "records", len(records))
		if single {
			return nil
		}
	}

	log.Infow("migrate index success", "piece", len(pieces))
	return nil
}

func migrateDeal(ctx context.Context, lstore *ldb.Store, ystore *yugabyte.Store) error {
	//List all pieces from leveldb
	pieces, err := lstore.ListPieces(ctx)
	if err != nil {
		return err
	}
	log.Infof("piece count in leveldb: %d", len(pieces))

	for _, piece := range pieces {
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

		log.Debugw("migrate deal", "piece", piece, "deals", deals)
	}

	log.Infow("migrate deal success", "piece", len(pieces))
	return nil
}

func migrateCheck(ctx context.Context, lstore *ldb.Store, ystore *yugabyte.Store, piece cid.Cid) error {
	{
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

		meta, err := lstore.GetPieceMetadata(ctx, piece)
		if err != nil {
			return err
		}

		log.Infow("migrate check", "piece", piece, "store", "leveldb", "records", len(records), "meta", meta)
	}

	{
		resp, err := ystore.GetIndex(ctx, piece)
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

		meta, err := ystore.GetPieceMetadata(ctx, piece)
		if err != nil {
			return err
		}

		log.Infow("migrate check", "piece", piece, "store", "yugabyte", "records", len(records), "meta", meta)
	}
	return nil
}
