// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"dragonflydb.io/profile/models"
)

func comparisonsCLI(ctx context.Context, flags *flag.FlagSet, args []string) error {
	base := flags.String("url", cmp.Or(os.Getenv("PROFILE_URL"), os.Getenv("RAFT_WORKFLOW_URL")), "platform URL")
	id := flags.String("id", "", "completed experiment ID")
	baseline := flags.String("baseline", "", "baseline target ID; empty clears it")
	previous := flags.String("previous", "", "previous experiment or target ID; empty clears it")
	reference := flags.String("reference", "", "reference target ID; empty clears it")
	asJSON := flags.Bool("json", false, "print summary JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *id == "" {
		return errors.New("comparisons requires -id ID")
	}
	client, err := newPlatformClient(*base)
	if err != nil {
		return err
	}
	path := experimentsAPI + "/" + url.PathEscape(*id)
	var summary models.Summary
	if err := client.request(ctx, http.MethodGet, path+"/summary", nil, &summary); err != nil {
		return err
	}
	choices := models.ChoiceIDs{}
	if c := summary.Comparisons.Cumulative; c != nil {
		choices.Cumulative = c.Target.ID
	}
	if c := summary.Comparisons.Previous; c != nil {
		choices.Previous = c.Target.ID
	}
	if c := summary.Comparisons.Reference; c != nil {
		choices.Reference = c.Target.ID
	}
	changed := false
	flags.Visit(func(flag *flag.Flag) {
		switch flag.Name {
		case "baseline":
			choices.Cumulative, changed = *baseline, true
		case "previous":
			choices.Previous, changed = *previous, true
		case "reference":
			choices.Reference, changed = *reference, true
		}
	})
	if changed {
		summary = models.Summary{}
		if err := client.request(ctx, http.MethodPut, path+"/comparisons", choices, &summary); err != nil {
			return err
		}
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(summary)
	}
	for _, item := range []struct {
		role   string
		choice *models.TargetComparison
	}{{"baseline", summary.Comparisons.Cumulative}, {"previous", summary.Comparisons.Previous}, {"reference", summary.Comparisons.Reference}} {
		if item.choice == nil {
			fmt.Printf("%s: unset\n", item.role)
			continue
		}
		fmt.Printf("%s: %s", item.role, item.choice.Target.ID)
		if result := item.choice.Result; result != nil {
			fmt.Printf("  throughput %+.2f%%  P95 %+.2f%%", result.Throughput, result.P95)
		}
		fmt.Println()
		if item.choice.Warning != "" {
			fmt.Println(" ", item.choice.Warning)
		}
	}
	return nil
}
