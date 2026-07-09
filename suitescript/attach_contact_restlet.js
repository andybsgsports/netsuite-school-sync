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
 * POST body (JSON):
 *   { "action": "attach",  "contactId": "8146", "customerId": "3551" }
 *   { "action": "detach",  "contactId": "8146", "customerId": "3551" }
 *
 * Response (JSON):
 *   { "success": true,  "action": "attach", "contactId": 8146, "customerId": 3551 }
 *   { "success": false, "error": "<NetSuite error message>" }
 *
 * GET (no params) is a health check:
 *   { "success": true, "service": "attach_contact_restlet", "version": 1 }
 */
define(['N/record', 'N/log'], (record, log) => {

    const post = (body) => {
        try {
            const action = String((body && body.action) || '').toLowerCase();
            const contactId = parseInt(body && body.contactId, 10);
            const customerId = parseInt(body && body.customerId, 10);

            if (!contactId || !customerId || (action !== 'attach' && action !== 'detach')) {
                return {
                    success: false,
                    error: 'Required fields: action ("attach"|"detach"), contactId, customerId',
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
    const get = () => ({ success: true, service: 'attach_contact_restlet', version: 1 });

    return { post: post, get: get };
});
