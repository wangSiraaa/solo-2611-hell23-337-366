-- Real SQLite read-back queries for the migration warehouse.
-- Usage:
--   sqlite3 warehouse.db < queries.sql
-- Every query is ordinary SQL over the materialised fact/declaration tables;
-- no XML is stored or reparsed at read time.

.headers on
.mode column
.nullvalue NULL

-- 1. Revision lineage: every committed revision and the DSD version it speaks.
SELECT r.revision_seq, r.kind, r.dsd_version_seq, d.dsd_id,
       r.plan_id, r.status, substr(r.content_hash, 1, 12) AS content_hash12
FROM revision r JOIN dsd_version d ON d.dsd_version_seq = r.dsd_version_seq
ORDER BY r.revision_seq;

-- 2. Current observation facts with the three value states kept distinct.
SELECT country, measure, time_period, value_state, raw_value, norm_value,
       isolated, reasons
FROM current_obs_fact
ORDER BY country, measure, time_period;

-- 3. Effective UNIT per observation at every revision, with attachment layer
--    and the originating declaration edge (proves no last-value overwrite).
SELECT f.revision_seq, f.obs_key, f.raw_value AS unit, f.explicit_empty,
       f.resolved_layer, f.source_edge_id, f.source_origin, f.source_revision
FROM v_attr_fact f
WHERE f.attr_id = 'UNIT'
ORDER BY f.revision_seq, f.obs_key;

-- 4. Coarsening/losslessness evidence: distinct UNIT values inside each
--    series (COUNTRY, MEASURE) at the current revision.
SELECT o.country, o.measure,
       COUNT(*) AS obs_in_series,
       COUNT(DISTINCT a.raw_value) AS distinct_units,
       GROUP_CONCAT(DISTINCT a.raw_value) AS units
FROM current_attr_fact a
JOIN current_obs_fact o ON o.obs_key = a.obs_key
WHERE a.attr_id = 'UNIT' AND a.present = 1
GROUP BY o.country, o.measure
ORDER BY o.country, o.measure;

-- 5. Isolated observations with the exact reason and revision.
SELECT revision_seq, obs_key, reasons
FROM v_obs_fact
WHERE isolated = 1
ORDER BY revision_seq, obs_key;

-- 6. Migration provenance: derived edge -> source edge -> covered observation.
SELECT mp.plan_id, mp.revision_seq, mp.derived_edge_id,
       de.attr_id, de.layer AS derived_layer, de.raw_value AS derived_value,
       mp.source_edge_id, se.layer AS source_layer, se.raw_value AS source_value,
       mp.obs_key
FROM migration_provenance mp
JOIN decl_edge de ON de.edge_id = mp.derived_edge_id
LEFT JOIN decl_edge se ON se.edge_id = mp.source_edge_id
ORDER BY mp.plan_id, mp.derived_edge_id, mp.obs_key;

-- 7. Append-only declaration history for one observation key.
SELECT obs_decl_id, revision_seq, operation, raw_value, explicit_empty, origin
FROM obs_decl
WHERE obs_key = '{"COUNTRY":"US","MEASURE":"VOL","TIME":"2026-02"}'
ORDER BY obs_decl_id;

-- 8. Compatibility assessment against every older DSD version.
SELECT revision_seq, target_dsd_version_seq, attr_id, expressible, reason,
       affected_obs_count
FROM compat_assessment
ORDER BY revision_seq, target_dsd_version_seq, attr_id;

-- 9. Materialised volume per revision (audit cross-check).
SELECT revision_seq,
       COUNT(*) AS obs_fact_rows,
       SUM(isolated) AS isolated_obs
FROM v_obs_fact
GROUP BY 1
ORDER BY 1;

-- 10. Plans and their bound/applied state.
SELECT plan_id, from_dsd_version_seq, to_dsd_version_seq,
       bound_revision_seq, status, applied_revision
FROM migration_plan
ORDER BY created_at;
