package mutationtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
)

type executionWorkspaces struct {
	repoRoot          string
	runDir            string
	ref               string
	dirs              []string
	selectedTestFiles []string
	testsPrepared     bool
}

func openExecutionWorkspaces(ctx context.Context, outputRoot string, ref string) (_ *executionWorkspaces, retErr error) {
	repoRoot, err := gitRepoRoot(ctx)
	if err != nil {
		return nil, err
	}
	fmt.Fprintln(os.Stderr, "[run] pruning stale git worktrees")
	if err := gitWorktreePrune(ctx, repoRoot); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(outputRoot) {
		outputRoot = filepath.Join(repoRoot, outputRoot)
	}
	runDir, err := prepareRunDir(ctx, outputRoot)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "[run] prepared output directory %s\n", displayPath(repoRoot, runDir))

	workspaces := &executionWorkspaces{
		repoRoot: repoRoot,
		runDir:   runDir,
		ref:      ref,
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, workspaces.close(ctx))
		}
	}()
	if err := workspaces.add(ctx); err != nil {
		return nil, err
	}
	return workspaces, nil
}

func (workspaces *executionWorkspaces) prepareTests(selectedTestFiles []string) error {
	if err := prepareTestFiles(workspaces.dirs[0], selectedTestFiles); err != nil {
		return err
	}
	workspaces.selectedTestFiles = slices.Clone(selectedTestFiles)
	workspaces.testsPrepared = true
	return nil
}

func (workspaces *executionWorkspaces) prepareWorkers(ctx context.Context, count int) error {
	if !workspaces.testsPrepared {
		return errors.New("workspace tests were not prepared")
	}
	for len(workspaces.dirs) < count {
		if err := workspaces.add(ctx); err != nil {
			return err
		}
		if err := prepareTestFiles(workspaces.dirs[len(workspaces.dirs)-1], workspaces.selectedTestFiles); err != nil {
			return err
		}
	}
	return nil
}

func (workspaces *executionWorkspaces) add(ctx context.Context) error {
	worktreeDir := filepath.Join(workspaces.runDir, fmt.Sprintf("worktree-%02d", len(workspaces.dirs)+1))
	workspaces.dirs = append(workspaces.dirs, worktreeDir)
	fmt.Fprintf(os.Stderr, "[run] creating worktree %s at %s\n", displayPath(workspaces.repoRoot, worktreeDir), workspaces.ref)
	if err := os.RemoveAll(worktreeDir); err != nil {
		return err
	}
	return gitWorktreeAdd(ctx, workspaces.repoRoot, worktreeDir, workspaces.ref)
}

func (workspaces *executionWorkspaces) close(ctx context.Context) error {
	cleanupCtx := context.WithoutCancel(ctx)
	var cleanupErrors []error
	for index := len(workspaces.dirs) - 1; index >= 0; index-- {
		if err := gitWorktreeRemove(cleanupCtx, workspaces.dirs[index]); err == nil {
			continue
		}
		cleanupErrors = append(cleanupErrors, os.RemoveAll(workspaces.dirs[index]))
	}
	cleanupErrors = append(cleanupErrors, gitWorktreePrune(cleanupCtx, workspaces.repoRoot))
	return errors.Join(cleanupErrors...)
}
