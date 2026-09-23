package oxinsider

// One default decision over a response's data_quality block
// (0xinsider/0xinsider#16965).
//
// data_quality says how old each part of a body is; it does not say whether
// that is old enough to act on, because the tolerance belongs to the caller.
// AssessDataQuality applies one. It reads the raw response bytes a
// ...WithResponse method keeps in Body, so it needs no generated type and
// makes no request. The TypeScript SDK (assessDataQuality) and the Python SDK
// (assess_data_quality) carry the same rule under the same name.
//
// The rule:
//   - A group passes only when its status is "fresh", it carries as_of, and
//     that clock is within MaxAge of Now. "fresh" alone means tracked and
//     clocked, not current enough for you.
//   - "untracked" groups are left out of the verdict: 0xinsider does not
//     track them for this subject by design. They are listed in Untracked so
//     a caller that needs one can refuse the body.
//   - Every other status fails: "unknown" (served, but this read cannot date
//     it; never treat missing as recent), "partial", "unavailable", and any
//     status this release does not recognize.
//   - A body with nothing to judge (every group untracked) is not OK.
//
// Owned outside the generated file so regeneration cannot change it.

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// DataQualityOptions sets the tolerance AssessDataQuality applies.
type DataQualityOptions struct {
	// MaxAge is the oldest clock you accept. It must not be negative.
	MaxAge time.Duration
	// Groups limits the verdict to these groups. A named group the body does
	// not carry fails with status "missing", so a renamed or removed group
	// cannot pass silently. Empty: judge every group the body carries.
	Groups []string
	// Now is the instant to measure against. Zero means time.Now().
	Now time.Time
}

// DataQualityFailure is one judged group that did not pass.
type DataQualityFailure struct {
	Group string
	// Status is the group's status, or "missing" when a requested group is
	// absent.
	Status string
	// Reason is the server's reason when it gave one, otherwise why this
	// helper failed the group.
	Reason string
	// AsOf is the group's clock as sent, empty when it carries none.
	AsOf string
	// Age is how far AsOf sits before Now; nil when the group has no clock.
	Age *time.Duration
}

// DataQualityAssessment is AssessDataQuality's verdict.
type DataQualityAssessment struct {
	// OK is true when at least one group was judged and every judged group
	// is fresh and within MaxAge.
	OK bool
	// Failing lists every judged group that did not pass, in response order.
	Failing []DataQualityFailure
	// Untracked lists the groups left out because they are "untracked".
	Untracked []string
	// OldestAsOf is the oldest as_of among the judged groups that carry one.
	OldestAsOf string
	// OldestAge is the age of OldestAsOf at Now; nil when no group has a clock.
	OldestAge *time.Duration
}

type dataQualityGroup struct {
	Group  string  `json:"group"`
	Status string  `json:"status"`
	AsOf   *string `json:"as_of"`
	Reason *string `json:"reason"`
}

type dataQualityBlock struct {
	Status      *string            `json:"status"`
	FieldGroups []dataQualityGroup `json:"field_groups"`
}

// locateDataQuality accepts the block itself, an object carrying it under
// data_quality (a list page, a trader's data), or an envelope carrying it
// under data.data_quality (a trader read).
func locateDataQuality(body []byte) (*dataQualityBlock, error) {
	var probe struct {
		DataQuality *dataQualityBlock `json:"data_quality"`
		Data        json.RawMessage   `json:"data"`
		dataQualityBlock
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return nil, fmt.Errorf("oxinsider: data_quality: %w", err)
	}
	if probe.DataQuality != nil {
		return probe.DataQuality, nil
	}
	if len(probe.Data) > 0 && probe.Data[0] == '{' {
		var inner struct {
			DataQuality *dataQualityBlock `json:"data_quality"`
		}
		if err := json.Unmarshal(probe.Data, &inner); err == nil && inner.DataQuality != nil {
			return inner.DataQuality, nil
		}
	}
	if probe.Status != nil && probe.FieldGroups != nil {
		block := probe.dataQualityBlock
		return &block, nil
	}
	return nil, errors.New("oxinsider: data_quality: the body carries no data_quality block")
}

// AssessDataQuality judges the data_quality block in body against opts.
//
//	resp, err := client.GetTraderWithResponse(ctx, "0xabc...", nil)
//	// handle err and a non-200 status first
//	verdict, err := oxinsider.AssessDataQuality(resp.Body, oxinsider.DataQualityOptions{MaxAge: 15 * time.Minute})
//	if err == nil && !verdict.OK {
//		for _, f := range verdict.Failing {
//			log.Println(f.Group, f.Status, f.Reason)
//		}
//	}
func AssessDataQuality(body []byte, opts DataQualityOptions) (DataQualityAssessment, error) {
	if opts.MaxAge < 0 {
		return DataQualityAssessment{}, errors.New("oxinsider: data_quality: MaxAge must not be negative")
	}
	block, err := locateDataQuality(body)
	if err != nil {
		return DataQualityAssessment{}, err
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}

	byName := make(map[string]dataQualityGroup, len(block.FieldGroups))
	names := opts.Groups
	for _, group := range block.FieldGroups {
		byName[group.Group] = group
	}
	if len(names) == 0 {
		for _, group := range block.FieldGroups {
			names = append(names, group.Group)
		}
	}

	var verdict DataQualityAssessment
	var judged []dataQualityGroup
	for _, name := range names {
		group, ok := byName[name]
		switch {
		case !ok:
			verdict.Failing = append(verdict.Failing, DataQualityFailure{
				Group: name, Status: "missing", Reason: "the response carries no such group",
			})
		case group.Status == "untracked":
			verdict.Untracked = append(verdict.Untracked, name)
		default:
			judged = append(judged, group)
		}
	}

	var oldest time.Time
	for _, group := range judged {
		failure := DataQualityFailure{Group: group.Group, Status: group.Status}
		var clock time.Time
		hasClock := false
		if group.AsOf != nil {
			failure.AsOf = *group.AsOf
			if parsed, parseErr := time.Parse(time.RFC3339Nano, *group.AsOf); parseErr == nil {
				clock, hasClock = parsed, true
				age := now.Sub(parsed)
				failure.Age = &age
				if oldest.IsZero() || parsed.Before(oldest) {
					oldest = parsed
					verdict.OldestAsOf = *group.AsOf
				}
			}
		}
		switch {
		case group.Status != "fresh":
			failure.Reason = "status is " + group.Status
			if group.Reason != nil && *group.Reason != "" {
				failure.Reason = *group.Reason
			}
		case !hasClock:
			failure.Reason = "fresh but carries no as_of, so no age can be measured"
		case now.Sub(clock) > opts.MaxAge:
			failure.Reason = fmt.Sprintf("older than MaxAge (%s)", opts.MaxAge)
		default:
			continue
		}
		verdict.Failing = append(verdict.Failing, failure)
	}

	if !oldest.IsZero() {
		age := now.Sub(oldest)
		verdict.OldestAge = &age
	}
	verdict.OK = len(verdict.Failing) == 0 && len(judged) > 0
	return verdict, nil
}
