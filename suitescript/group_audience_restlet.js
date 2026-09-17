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
 * Deliberately a SEPARATE script from attach_contact_restlet.js, which
 * the nightly sync depends on every run — a bug here can never take that
 * down. See RESTLET_SETUP.md for the one-time deploy steps; use the same
 * pattern with a NEW script record (name it e.g. "BSG Group Audience")
 * and two NEW GitHub secrets, NS_GROUP_RESTLET_SCRIPT_ID /
 * NS_GROUP_RESTLET_DEPLOY_ID.
 *
 * POST body (JSON):
 *   { "action": "inspect", "groupIds": [93697, ...] }
 *     - read-only. For each group: its name, the linked saved search's
 *       internal id/title, current filters and columns (raw), and
 *       whether an isinactive=F filter and a company-ish column are
 *       already present. Never calls .save().
 *   { "action": "fix", "groupIds": [93697, ...], "dryRun": true }
 *     - dryRun (default true): same report as "inspect" plus a "would
 *       change" field per group — never calls .save().
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
define(['N/record', 'N/search', 'N/log'], (record, search, log) => {

    // The group record's field holding the linked saved search's internal
    // id — try a few plausible ids since this isn't documented for the
    // standard "group" record type; the response reports which one hit so
    // a mismatch is visible immediately instead of silently no-op'ing.
    const SAVEDSEARCH_FIELD_CANDIDATES = [
        'savedsearch', 'contactsavedsearchid', 'savedsearchid', 'searchid',
    ];

    const loadGroupSavedSearchId = (groupId) => {
        const rec = record.load({ type: 'group', id: groupId, isDynamic: false });
        const name = rec.getValue({ fieldId: 'groupname' }) || rec.getValue({ fieldId: 'name' });
        for (const fld of SAVEDSEARCH_FIELD_CANDIDATES) {
            let val;
            try { val = rec.getValue({ fieldId: fld }); } catch (e) { continue; }
            if (val) return { groupName: name, searchId: val, fieldUsed: fld };
        }
        return { groupName: name, searchId: null, fieldUsed: null };
    };

    const describeFilter = (f) => ({
        name: f.name, operator: f.operator, join: f.join || null,
        values: f.values !== undefined ? f.values : null,
    });
    const describeColumn = (c) => ({
        name: c.name, join: c.join || null, label: c.label || null,
    });

    const inspectOne = (groupId) => {
        const out = { groupId };
        let g;
        try {
            g = loadGroupSavedSearchId(groupId);
        } catch (e) {
            out.error = 'group load failed: ' + ((e && e.message) || String(e));
            return out;
        }
        out.groupName = g.groupName;
        out.savedSearchFieldUsed = g.fieldUsed;
        out.savedSearchId = g.searchId;
        if (!g.searchId) {
            out.error = 'no savedsearch id found on group record (tried: '
                + SAVEDSEARCH_FIELD_CANDIDATES.join(', ') + ')';
            return out;
        }
        let s;
        try {
            s = search.load({ id: g.searchId });
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

    const fixOne = (groupId, dryRun) => {
        const out = inspectOne(groupId);
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
            const groupIds = Array.isArray(body && body.groupIds) ? body.groupIds : [];
            const dryRun = !(body && body.dryRun === false); // default true

            if (!groupIds.length || (action !== 'inspect' && action !== 'fix')) {
                return { success: false,
                    error: 'Required fields: action ("inspect"|"fix"), groupIds: [ids]' };
            }

            const results = groupIds.map((gid) => {
                const r = action === 'inspect' ? inspectOne(gid) : fixOne(gid, dryRun);
                if (r._searchObj) delete r._searchObj;
                return r;
            });

            log.audit('group_audience_restlet',
                `${action} dryRun=${dryRun} groups=${groupIds.join(',')}`);
            return { success: true, action: action, dryRun: dryRun, results: results };

        } catch (e) {
            const msg = (e && e.message) ? e.message : String(e);
            log.error('group_audience_restlet', msg);
            return { success: false, error: msg };
        }
    };

    const get = () => ({ success: true, service: 'group_audience_restlet', version: 1 });

    return { post: post, get: get };
});
