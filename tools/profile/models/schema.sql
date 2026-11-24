CREATE SCHEMA IF NOT EXISTS raft_workflow;

CREATE TABLE IF NOT EXISTS raft_workflow.schema_migrations (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raft_workflow.experiments (
    id text PRIMARY KEY,
    state text NOT NULL CHECK (state IN ('draft', 'complete')),
    summary jsonb NOT NULL,
    asset_dir text NOT NULL,
    manifest_sha256 text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raft_workflow.comparison_targets (
    id text PRIMARY KEY,
    benchmark jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS raft_workflow.executions (
    experiment_id text NOT NULL REFERENCES raft_workflow.experiments(id) ON DELETE CASCADE,
    attempt integer NOT NULL CHECK (attempt > 0),
    status text NOT NULL CHECK (status IN ('running', 'failed', 'complete')),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    error text NOT NULL DEFAULT '',
    PRIMARY KEY (experiment_id, attempt)
);

CREATE TABLE IF NOT EXISTS raft_workflow.experiment_comparisons (
    experiment_id text NOT NULL REFERENCES raft_workflow.experiments(id) ON DELETE CASCADE,
    role text NOT NULL CHECK (role IN ('cumulative_baseline', 'reference', 'previous')),
    target_id text NOT NULL,
    comparison jsonb NOT NULL,
    PRIMARY KEY (experiment_id, role)
);

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_constraint WHERE conrelid='raft_workflow.experiment_comparisons'::regclass
                   AND conname='experiment_comparison_target_fk') THEN
        ALTER TABLE raft_workflow.experiment_comparisons ADD CONSTRAINT experiment_comparison_target_fk
            FOREIGN KEY (target_id) REFERENCES raft_workflow.comparison_targets(id)
            DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS raft_workflow.benchmark_sets (
    experiment_id text NOT NULL REFERENCES raft_workflow.experiments(id) ON DELETE CASCADE,
    id text NOT NULL,
    configuration jsonb NOT NULL,
    PRIMARY KEY (experiment_id, id)
);

CREATE TABLE IF NOT EXISTS raft_workflow.benchmark_variants (
    experiment_id text NOT NULL,
    set_id text NOT NULL,
    variant text NOT NULL,
    benchmark jsonb NOT NULL,
    PRIMARY KEY (experiment_id, set_id, variant),
    FOREIGN KEY (experiment_id, set_id) REFERENCES raft_workflow.benchmark_sets(experiment_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS raft_workflow.runs (
    experiment_id text NOT NULL REFERENCES raft_workflow.experiments(id) ON DELETE CASCADE,
    id text NOT NULL,
    position integer NOT NULL,
    kind text NOT NULL CHECK (kind IN ('benchmark', 'diagnostic')),
    benchmark_set text,
    variant text NOT NULL,
    trial integer,
    replication_mib_per_s double precision NOT NULL,
    batch_p95_ms double precision NOT NULL,
    run jsonb NOT NULL,
    measurement jsonb,
    PRIMARY KEY (experiment_id, id),
    UNIQUE (experiment_id, position),
    FOREIGN KEY (experiment_id, benchmark_set, variant) REFERENCES raft_workflow.benchmark_variants(experiment_id, set_id, variant)
);

CREATE TABLE IF NOT EXISTS raft_workflow.diagnostics (
    experiment_id text NOT NULL REFERENCES raft_workflow.experiments(id) ON DELETE CASCADE,
    variant text NOT NULL,
    observation jsonb NOT NULL,
    profile jsonb NOT NULL,
    PRIMARY KEY (experiment_id, variant)
);

CREATE TABLE IF NOT EXISTS raft_workflow.trace_stages (
    experiment_id text NOT NULL,
    variant text NOT NULL,
    node text NOT NULL,
    stage text NOT NULL,
    count integer NOT NULL,
    union_ms double precision NOT NULL,
    statistics jsonb NOT NULL,
    PRIMARY KEY (experiment_id, variant, node, stage),
    FOREIGN KEY (experiment_id, variant) REFERENCES raft_workflow.diagnostics(experiment_id, variant) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS raft_workflow.validation_checks (
    experiment_id text NOT NULL REFERENCES raft_workflow.experiments(id) ON DELETE CASCADE,
    build text NOT NULL,
    suite text NOT NULL,
    tests integer NOT NULL,
    check_result jsonb NOT NULL,
    PRIMARY KEY (experiment_id, build, suite)
);

CREATE TABLE IF NOT EXISTS raft_workflow.artifacts (
    experiment_id text NOT NULL REFERENCES raft_workflow.experiments(id) ON DELETE CASCADE,
    id text NOT NULL,
    filename text NOT NULL,
    bytes bigint NOT NULL,
    PRIMARY KEY (experiment_id, id)
);

CREATE OR REPLACE VIEW raft_workflow.experiment_summaries AS
SELECT e.id, e.state, e.asset_dir,
       e.summary || jsonb_build_object(
           'variants', COALESCE((SELECT jsonb_object_agg(variant, benchmark) FROM raft_workflow.benchmark_variants b
                                 WHERE b.experiment_id=e.id AND set_id='main'), '{}'::jsonb),
           'comparisons', COALESCE((SELECT jsonb_object_agg(role, comparison) FROM raft_workflow.experiment_comparisons c
                                     WHERE c.experiment_id=e.id), '{}'::jsonb),
           'diagnostics', COALESCE((SELECT jsonb_object_agg(variant, profile) FROM raft_workflow.diagnostics d
                                     WHERE d.experiment_id=e.id), '{}'::jsonb)) AS summary
FROM raft_workflow.experiments e;

INSERT INTO raft_workflow.schema_migrations (version) VALUES (1) ON CONFLICT DO NOTHING;
INSERT INTO raft_workflow.schema_migrations (version) VALUES (2) ON CONFLICT DO NOTHING;
INSERT INTO raft_workflow.schema_migrations (version) VALUES (3) ON CONFLICT DO NOTHING;
