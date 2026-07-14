/**
 * @NApiVersion 2.1
 * @NScriptType Restlet
 * @NModuleScope SameAccount
 *
 * send_email_restlet.js — lets the weekly scores digest (scores_email.py)
 * send email through NetSuite so it goes out from andy@bsgsports.com,
 * exactly like emails sent from the NetSuite UI. This avoids needing
 * Microsoft admin consent for Graph Mail.Send or a Gmail alias.
 *
 * POST body (JSON):
 *   {
 *     "recipient": "andy@bsgsports.com",        // or comma-separated list
 *     "subject":   "BSG Sports Scores — ...",
 *     "htmlBody":  "<html>...</html>",
 *     "authorId":  "123"                        // optional employee internal
 *   }                                           // id; defaults to the token's
 *                                               // own user
 *
 * Response (JSON):
 *   { "success": true,  "author": 123, "recipients": ["andy@bsgsports.com"] }
 *   { "success": false, "error": "<NetSuite error message>" }
 *
 * GET (no params) is a health check:
 *   { "success": true, "service": "send_email_restlet", "version": 1,
 *     "tokenUser": <employee id the integration token runs as> }
 */
define(['N/email', 'N/runtime', 'N/log'], (email, runtime, log) => {

    const post = (body) => {
        try {
            const b = (typeof body === 'string') ? JSON.parse(body) : (body || {});
            const subject  = String(b.subject || '').trim();
            const htmlBody = String(b.htmlBody || '').trim();
            const recipients = String(b.recipient || '')
                .split(',').map((s) => s.trim()).filter(Boolean);

            if (!subject || !htmlBody || recipients.length === 0) {
                return {
                    success: false,
                    error: 'Required fields: recipient, subject, htmlBody',
                };
            }

            // From address = this employee's email on their NetSuite record.
            const author = parseInt(b.authorId, 10) || runtime.getCurrentUser().id;

            email.send({
                author: author,
                recipients: recipients,
                subject: subject,
                body: htmlBody,
            });

            log.audit('send_email_restlet',
                `sent "${subject}" to ${recipients.join(', ')} as employee ${author}`);
            return { success: true, author: author, recipients: recipients };

        } catch (e) {
            const msg = (e && e.message) ? e.message : String(e);
            log.error('send_email_restlet', msg);
            return { success: false, error: msg };
        }
    };

    // Health check so the sync can verify the deployment is reachable.
    const get = () => ({
        success: true,
        service: 'send_email_restlet',
        version: 1,
        tokenUser: runtime.getCurrentUser().id,
    });

    return { post: post, get: get };
});
