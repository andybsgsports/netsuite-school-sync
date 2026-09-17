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
 * Same day, Andy also asked each of the 13 searches be scoped to only
 * contacts at schools where HE is the Sales Rep (NetSuite employee id 3
 * — see diag_salesrep_field.py, confirmed Customer.salesrep mirrors the
 * salesTeam sublist netsuite_sync.py sets). Adds a
 * company.salesrep = <salesRepId> filter alongside the inactive one,
 * same idempotent add-if-missing pattern.
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
 * Also confirmed live (2026-09-17): the Group's own "Members" tab in the
 * NetSuite UI is a FIXED display (Name/Phone/Email/Bounced/Inactive/
 * Subscription Status) that ignores the linked search's own Results
 * columns entirely — Boys Football Coaches still showed a Phone column
 * there with zero phone columns in its search definition. Nothing here
 * (or in the search definition) can change what that specific screen
 * shows. Adding a Company column to the search DOES show up when the
 * search itself is opened directly (Lists > Search > Saved Searches) or
 * exported to CSV — that's the real workaround for "which school is
 * this contact at".
 *
 * POST body (JSON):
 *   { "action": "inspect", "searches": { "Boys Football Coaches": 12345, ... } }
 *     - read-only. For each label->id pair: the search's title/type,
 *       current filters/columns, current result count, and whether an
 *       isinactive=F filter / sales rep filter / company column are
 *       already present. Never calls .save().
 *   { "action": "fix", "searches": {...}, "dryRun": true, "salesRepId": "3" }
 *     - salesRepId optional — omit to only fix the inactive filter.
 *     - dryRun (default true): same report as "inspect" plus a "would
 *       change" field and a "wouldBeResultCount" (computed by running
 *       the search with the candidate filters applied in-memory, before
 *       any .save()). Never calls .save().
 *     - dryRun: false: adds an isinactive=F filter if missing, adds a
 *       company.salesrep=salesRepId filter if provided and missing, and
 *       adds a "company" column if not already present, then .save()s
 *       the search. Idempotent — running it again on an already-fixed
 *       search changes nothing.
 *
 * Response: { "success": true, "action": ..., "results": [ {...} ] }
 *        or { "success": false, "error": "..." }
 *
 * GET (no params) is a health check.
 */
define(['N/search', 'N/log'], (search, log) => {

    // NOTE: N/search's Filter.values comes back null when reading filters
    // off a LOADED (already-saved) search — confirmed live, 2026-09-17:
    // even the working isinactive filter reports values:null. Detection
    // below matches on name+join only, never values, for exactly that
    // reason (an earlier version compared values here, which made
    // hasSalesRepFilter always false and appended a harmless-but-sloppy
    // duplicate filter on every run).
    const describeFilter = (f) => ({
        name: f.name, operator: f.operator, join: f.join || null,
        values: f.values !== undefined ? f.values : null,
    });
    const describeColumn = (c) => ({
        name: c.name, join: c.join || null, label: c.label || null,
    });

    const isInactiveFilter = (f) => f.name === 'isinactive';
    const isSalesRepFilter = (f) => f.name === 'salesrep' && f.join === 'company';

    const resultCount = (s) => {
        try {
            return s.runPaged({ pageSize: 1000 }).count;
        } catch (e) {
            return null; // don't let a count failure block the real report
        }
    };

    const inspectOne = (label, searchId, salesRepId) => {
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
        out.inactiveFilterCount = out.filters.filter(isInactiveFilter).length;
        out.salesRepFilterCount = out.filters.filter(isSalesRepFilter).length;
        out.hasInactiveFilter = out.inactiveFilterCount > 0;
        out.hasSalesRepFilter = salesRepId ? out.salesRepFilterCount > 0 : null;
        out.hasCompanyColumn = out.columns.some(c => c.name === 'company');
        out.currentResultCount = resultCount(s);
        out._searchObj = s; // stripped before response; used by fixOne
        return out;
    };

    const fixOne = (label, searchId, salesRepId, dryRun) => {
        const out = inspectOne(label, searchId, salesRepId);
        if (out.error) return out;
        const s = out._searchObj;
        delete out._searchObj;

        // Full rebuild, not incremental concat — deterministic and
        // self-healing: ends up with EXACTLY one isinactive filter and
        // (if salesRepId given) exactly one company.salesrep filter,
        // whether the search currently has zero, one, or (as a couple
        // did, from the values:null bug above) two of them.
        const willFixInactiveFilter = out.inactiveFilterCount !== 1;
        const willFixSalesRepFilter = !!salesRepId && out.salesRepFilterCount !== 1;
        // Unconditional — not just when a phone column happens to be
        // present. The Group's own "Members" tab is a fixed NetSuite
        // display (Name/Phone/Email/Bounced/Inactive/Subscription) that
        // ignores the search's columns entirely (confirmed live,
        // 2026-09-17: Boys Football Coaches still shows Phone with zero
        // phone columns in the search def) — this can't fix that screen.
        // It DOES make Company show up when the search is opened
        // directly (Lists > Search > Saved Searches) or exported to CSV.
        const willAddCompanyColumn = !out.hasCompanyColumn;
        out.wouldChange = { fixInactiveFilter: willFixInactiveFilter,
                             fixSalesRepFilter: willFixSalesRepFilter,
                             addCompanyColumn: willAddCompanyColumn };

        const nothingToDo = !willFixInactiveFilter && !willFixSalesRepFilter && !willAddCompanyColumn;
        if (nothingToDo) {
            out.applied = false;
            out.wouldBeResultCount = out.currentResultCount;
            return out;
        }

        // Apply candidate changes to the in-memory search object first —
        // running it (no .save() yet) shows the real before/after impact,
        // in dry run too, without persisting anything.
        if (willFixInactiveFilter) {
            s.filters = (s.filters || []).filter(f => !isInactiveFilter(f)).concat(
                search.createFilter({ name: 'isinactive', operator: search.Operator.IS, values: 'F' }));
        }
        if (willFixSalesRepFilter) {
            s.filters = (s.filters || []).filter(f => !isSalesRepFilter(f)).concat(
                search.createFilter({ name: 'salesrep', join: 'company',
                                       operator: search.Operator.ANYOF, values: [salesRepId] }));
        }
        if (willAddCompanyColumn) {
            s.columns = (s.columns || []).concat(search.createColumn({ name: 'company' }));
        }
        out.wouldBeResultCount = resultCount(s);

        if (dryRun) {
            out.applied = false;
            return out;
        }

        try {
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
            const salesRepId = (body && body.salesRepId) ? String(body.salesRepId) : null;

            if (!labels.length || (action !== 'inspect' && action !== 'fix')) {
                return { success: false,
                    error: 'Required fields: action ("inspect"|"fix"), searches: {"label": searchId, ...}' };
            }

            const results = labels.map((label) => {
                const sid = parseInt(searches[label], 10);
                const r = action === 'inspect' ? inspectOne(label, sid, salesRepId)
                                                : fixOne(label, sid, salesRepId, dryRun);
                if (r._searchObj) delete r._searchObj;
                return r;
            });

            log.audit('group_audience_restlet',
                `${action} dryRun=${dryRun} salesRepId=${salesRepId} searches=${labels.join(',')}`);
            return { success: true, action: action, dryRun: dryRun, results: results };

        } catch (e) {
            const msg = (e && e.message) ? e.message : String(e);
            log.error('group_audience_restlet', msg);
            return { success: false, error: msg };
        }
    };

    const get = () => ({ success: true, service: 'group_audience_restlet', version: 5 });

    return { post: post, get: get };
});
