/**
 * @NApiVersion 2.1
 * @NScriptType Restlet
 * @NModuleScope SameAccount
 *
 * group_audience_restlet.js — fixes the "Email Audience" saved searches
 * behind the coach Groups (Lists > Relationships > Groups: Baseball
 * Coaches, Boys Football Coaches, Athletic Directors, ...).
 *
 * Why this exists: those groups are DYNAMIC — each one's membership is
 * recomputed live from its linked saved search, not stored anywhere. On
 * 2026-09-17 Andy found a retired duplicate contact ("Ryan McKittrick
 * (dup 54939)", isInactive=Yes) still listed as a member of "Boys
 * Football Coaches". The saved search has no "Inactive = No" filter, so
 * it shows every duplicate this sync ever retires — permanently, not
 * just the ones from the September cleanup. Adding that one filter to
 * each search fixes it for good; NetSuite recomputes the group from the
 * search on every view/send, so there's nothing to keep re-running.
 *
 * v1 tried to find each search's internal id by loading the linked
 * Group record (record.load({type:'group', ...})) — NetSuite rejected
 * that with "The record type [GROUP] is invalid": CRM Group isn't a
 * record type SuiteScript's record module supports at all (mirrors the
 * REST record API, which doesn't expose it either). v2 skips the group
 * record entirely and takes each saved search's internal id directly —
 * N/search.load() is well-supported and needs nothing else.
 *
 * Deliberately a SEPARATE script from attach_contact_restlet.js, which
 * the nightly sync depends on every run — a bug here can never take that
 * down.
 *
 * POST body (JSON):
 *   { "action": "inspect", "searches": { "Boys Football Coaches": 12345, ... } }
 *     - read-only. For each label->id pair: the search's title/type,
 *       current filters and columns (raw), and whether an isinactive=F
 *       filter and a company-ish column are already present. Never
 *       calls .save().
 *   { "action": "fix", "searches": {...}, "dryRun": true }
 *     - dryRun (default true): same report as "inspect" plus a "would
 *       change" field per search — never calls .save().
 *     - dryRun: false: adds an isinactive=F filter if missing, and swaps
 *       a "phone" column for a "company" column if phone is present and
 *       company isn't, then .save()s the search. Idempotent — running it
 *       again on an already-fixed search changes nothing.
 *
 * Response: { "success": true, "action": ..., "results": [ {...} ] }
 *        or { "success": false, "error": "..." }
 *
 * GET (no params) is a health check.
 */
define(['N/search', 'N/log'], (search, log) => {

    const describeFilter = (f) => ({
        name: f.name, operator: f.operator, join: f.join || null,
        values: f.values !== undefined ? f.values : null,
    });
    const describeColumn = (c) => ({
        name: c.name, join: c.join || null, label: c.label || null,
    });

    const inspectOne = (label, searchId) => {
        const out = { label: label, savedSearchId: searchId };
        let s;
        try {
            s = search.load({ id: searchId });
        } catch (e) {
            out.error = 'search.load failed: ' + ((e && e.message) || String(e));
            return out;
        }
        out.searchTitle = s.title;
        out.searchType = s.searchType;
        out.filters = (s.filters || []).map(describeFilter);
        out.columns = (s.columns || []).map(describeColumn);
        out.hasInactiveFilter = out.filters.some(f => f.name === 'isinactive');
        out.hasPhoneColumn = out.columns.some(c => c.name === 'phone');
        out.hasCompanyColumn = out.columns.some(c => c.name === 'company');
        out._searchObj = s; // stripped before response; used by fixOne
        return out;
    };

    const fixOne = (label, searchId, dryRun) => {
        const out = inspectOne(label, searchId);
        if (out.error) return out;
        const s = out._searchObj;
        delete out._searchObj;

        const willAddInactiveFilter = !out.hasInactiveFilter;
        const willSwapColumn = out.hasPhoneColumn && !out.hasCompanyColumn;
        out.wouldChange = { addInactiveFilter: willAddInactiveFilter,
                             swapPhoneForCompanyColumn: willSwapColumn };

        if (dryRun || (!willAddInactiveFilter && !willSwapColumn)) {
            out.applied = false;
            return out;
        }

        try {
            if (willAddInactiveFilter) {
                s.filters = (s.filters || []).concat(
                    search.createFilter({ name: 'isinactive', operator: search.Operator.IS, values: 'F' }));
            }
            if (willSwapColumn) {
                s.columns = (s.columns || [])
                    .filter(c => c.name !== 'phone')
                    .concat(search.createColumn({ name: 'company' }));
            }
            s.save();
            out.applied = true;
        } catch (e) {
            out.applied = false;
            out.error = 'save failed: ' + ((e && e.message) || String(e));
        }
        return out;
    };

    const post = (body) => {
        try {
            const action = String((body && body.action) || '').toLowerCase();
            const searches = (body && body.searches) || {};
            const labels = Object.keys(searches);
            const dryRun = !(body && body.dryRun === false); // default true

            if (!labels.length || (action !== 'inspect' && action !== 'fix')) {
                return { success: false,
                    error: 'Required fields: action ("inspect"|"fix"), searches: {"label": searchId, ...}' };
            }

            const results = labels.map((label) => {
                const sid = parseInt(searches[label], 10);
                const r = action === 'inspect' ? inspectOne(label, sid) : fixOne(label, sid, dryRun);
                if (r._searchObj) delete r._searchObj;
                return r;
            });

            log.audit('group_audience_restlet',
                `${action} dryRun=${dryRun} searches=${labels.join(',')}`);
            return { success: true, action: action, dryRun: dryRun, results: results };

        } catch (e) {
            const msg = (e && e.message) ? e.message : String(e);
            log.error('group_audience_restlet', msg);
            return { success: false, error: msg };
        }
    };

    const get = () => ({ success: true, service: 'group_audience_restlet', version: 2 });

    return { post: post, get: get };
});
