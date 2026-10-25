# Markdown dependencies

These browser distributions are served locally; the note editor does not require a CDN.

- [Marked 18.1.1](https://github.com/markedjs/marked/releases/tag/v18.1.1): `https://cdn.jsdelivr.net/npm/marked@18.1.1/lib/marked.umd.js`; MIT license in `marked-LICENSE.md`.
- [DOMPurify 3.4.17](https://github.com/cure53/DOMPurify/releases/tag/3.4.17): `https://cdn.jsdelivr.net/npm/dompurify@3.4.17/dist/purify.min.js`; dual Apache-2.0 / MPL-2.0 license in `DOMPurify-LICENSE`.

`stock-notes.js` parses Markdown, then sanitizes the output using an explicit allowlist of text, table and link elements before inserting a DOM fragment. Scripts, images, embedded pages, styles and form controls are not allowed.
