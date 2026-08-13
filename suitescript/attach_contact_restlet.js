/**
 * @NApiVersion 2.1
 * @NScriptType Restlet
 * @NModuleScope SameAccount
 *
 * attach_contact_restlet.js — exposes NetSuite's native contact attach/detach
 * (the same mechanism as the UI's "Attach" button on a customer's
 * Relationships > Contacts tab) to the nightly sync.
 *
 * Why this exists: the SuiteTalk REST record API treats a customer's
 * contactRoles as a READ-ONLY ("static") sublist — POST to the
 * sub-collection returns 405 and nested PATCH returns 400 "you are trying to
 * add or remove lines from a static sublist" (both verified against this
 * account). So one contact card shared across multiple schools (co-op
 * coaches like Tony Brewer at Albany + Monticello, or Bret St. Arnauld at
 * Mt. Horeb + Barneveld) can only be wired up through SuiteScript's
 * record.attach / record.detach. This RESTlet is the thin bridge that lets
 * the Python sync call it.
 *
 * Also exposes address-line REMOVAL. The REST record API cannot delete an
 * addressBook line (a customer-level PATCH only ever ADDS), so a departed
 * contact's Ship-To used to be relabeled "(Removed) <name>" and left on the
 * record forever — cluttering the Ship To dropdown on every quote/order.
 * SuiteScript's removeLine deletes it outright.
 *
 * POST body (JSON):
 *   { "action": "attach",  "contactId": "8146", "customerId": "3551" }
 *   { "action": "detach",  "contactId": "8146", "customerId": "3551" }
 *   { "action": "removeAddress", "customerId": "3551", "label": "Jordan Reynolds" }
 *     - matches the addressBook line whose label equals `label`
 *       (case-insensitive), including a legacy "(Removed) " prefix.
 *     - "labelPrefix" instead of "label" removes every line whose label
 *       starts with that prefix — used by the one-time "(Removed) " purge.
 *
 * Response (JSON):
 *   { "success": true,  "action": "attach", "contactId": 8146, "customerId": 3551 }
 *   { "success": true,  "action": "removeAddress", "removed": 2 }
 *   { "success": false, "error": "<NetSuite error message>" }
 *
 * GET (no params) is a health check:
 *   { "success": true, "service": "attach_contact_restlet", "version": 2 }
 */
define(['N/record', 'N/log'], (record, log) => {

    /**
     * Delete addressBook lines from a customer by label. Iterates backwards so
     * removing a line doesn't shift the indexes of ones not yet checked.
     * Returns the number of lines removed.
     */
    const removeAddressLines = (customerId, label, labelPrefix) => {
        const want = label ? String(label).trim().toLowerCase() : null;
        const prefix = labelPrefix ? String(labelPrefix).trim().toLowerCase() : null;
        const rec = record.load({
            type: record.Type.CUSTOMER, id: customerId, isDynamic: false,
        });
        const count = rec.getLineCount({ sublistId: 'addressbook' });
        let removed = 0;
        for (let i = count - 1; i >= 0; i--) {
            const raw = rec.getSublistValue({
                sublistId: 'addressbook', fieldId: 'label', line: i,
            });
            const lbl = String(raw || '').trim().toLowerCase();
            if (!lbl) continue;
            // A legacy line may already carry the "(removed) " prefix; treat
            // "(Removed) Jane Doe" as a match for label "Jane Doe" so the
            // nightly run cleans up its own past output.
            const bare = lbl.replace(/^\(removed\)\s*/, '');
            const hit = prefix ? lbl.indexOf(prefix) === 0
                              : (lbl === want || bare === want);
            if (hit) {
                rec.removeLine({ sublistId: 'addressbook', line: i });
                removed++;
            }
        }
        if (removed) rec.save({ ignoreMandatoryFields: true });
        return removed;
    };

    const post = (body) => {
        try {
            const action = String((body && body.action) || '').toLowerCase();
            const contactId = parseInt(body && body.contactId, 10);
            const customerId = parseInt(body && body.customerId, 10);

            if (action === 'removeaddress') {
                const label = body && body.label;
                const labelPrefix = body && body.labelPrefix;
                if (!customerId || (!label && !labelPrefix)) {
                    return {
                        success: false,
                        error: 'removeAddress requires customerId and label (or labelPrefix)',
                    };
                }
                const removed = removeAddressLines(customerId, label, labelPrefix);
                log.audit('attach_contact_restlet',
                    `removeAddress: customer ${customerId} label=${label || labelPrefix} removed=${removed}`);
                return { success: true, action: 'removeAddress',
                         customerId: customerId, removed: removed };
            }

            if (!contactId || !customerId || (action !== 'attach' && action !== 'detach')) {
                return {
                    success: false,
                    error: 'Required fields: action ("attach"|"detach"|"removeAddress"), contactId, customerId',
                };
            }

            if (action === 'attach') {
                record.attach({
                    record: { type: record.Type.CONTACT, id: contactId },
                    to: { type: record.Type.CUSTOMER, id: customerId },
                });
            } else {
                record.detach({
                    record: { type: record.Type.CONTACT, id: contactId },
                    from: { type: record.Type.CUSTOMER, id: customerId },
                });
            }

            log.audit('attach_contact_restlet',
                `${action}: contact ${contactId} <-> customer ${customerId}`);
            return { success: true, action: action, contactId: contactId, customerId: customerId };

        } catch (e) {
            const msg = (e && e.message) ? e.message : String(e);
            log.error('attach_contact_restlet', msg);
            return { success: false, error: msg };
        }
    };

    // Health check so the sync can verify the deployment is reachable.
    const get = () => ({ success: true, service: 'attach_contact_restlet', version: 2 });

    return { post: post, get: get };
});
