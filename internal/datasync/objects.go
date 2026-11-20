package datasync

import (
	"context"

	"github.com/zoctl/cli/internal/transfer"
	"github.com/zoctl/cli/internal/transfer/objects"
)

// objectSet is a bucket copied to a bucket.
type objectSet struct {
	pair *objects.Pair
	// back is the target copied to the old side, after a cutover.
	back bool
	// checkOnly is a reverse set opened to compare: the old side came in
	// on the source's read-only keys, so it is never written.
	checkOnly bool
}

// newObjectSet pairs a source with a target. The source goes in read-only.
func newObjectSet(label, prefix string, src objects.Reader, dst objects.Store) *objectSet {
	return &objectSet{pair: objects.NewPair(objects.ReadOnly(src), dst, label, prefix)}
}

func openDataset(d Dataset, o Options) (dataset, error) {
	switch d.Kind {
	case "objects":
		src, err := objects.Open(d.From, "SOURCE", o.Env, o.Client)
		if err != nil {
			return nil, err
		}
		dst, err := objects.Open(d.To, "TARGET", o.Env, o.Client)
		if err != nil {
			return nil, err
		}
		// The label is the pair of specs, so the plan's digest names the
		// buckets without the plan holding them.
		return newObjectSet(d.From+"\x00"+d.To, d.Prefix, src, dst), nil
	case "postgres":
		return openPostgres(d, o)
	}
	return nil, ErrMove
}

func (o *objectSet) Plan(ctx context.Context) (DatasetPlan, error) {
	p, err := o.pair.Plan(ctx)
	if err != nil {
		return DatasetPlan{}, err
	}
	return DatasetPlan{Kind: "objects", Copy: int64(len(p.Copy)), CopyBytes: p.CopyBytes, Same: p.Same, Extra: p.Extra, Digest: p.Digest, work: p.Copy}, nil
}

func (o *objectSet) Start(ctx context.Context, p DatasetPlan, b *transfer.Budget) (DatasetStatus, error) {
	if o.checkOnly {
		return DatasetStatus{Kind: "objects"}, ErrMove
	}
	list, _ := p.work.([]objects.Object)
	prog, err := o.pair.Copy(ctx, list, b)
	return DatasetStatus{Kind: "objects", Copied: prog.Copied, Bytes: prog.Bytes, Failed: prog.Failed, Pending: int64(len(list)) - prog.Copied, Extra: p.Extra}, err
}

func (o *objectSet) Round(ctx context.Context, b *transfer.Budget) (DatasetStatus, error) {
	if o.checkOnly {
		return DatasetStatus{Kind: "objects"}, ErrMove
	}
	r, err := o.pair.Round(ctx, b)
	return DatasetStatus{Kind: "objects", Copied: r.Copied, Bytes: r.Bytes, Failed: r.Failed, Pending: r.Planned - r.Copied, Extra: r.Extra}, err
}

func (o *objectSet) Check(ctx context.Context) (DatasetStatus, error) {
	v, err := o.pair.Verify(ctx)
	if err != nil {
		return DatasetStatus{}, err
	}
	return DatasetStatus{Kind: "objects", Pending: v.Missing + v.Different, Extra: v.Extra, Verified: v.OK(), extras: v.Extras}, nil
}

func (o *objectSet) Stop(context.Context) error { return nil }
func (o *objectSet) Close()                     {}
