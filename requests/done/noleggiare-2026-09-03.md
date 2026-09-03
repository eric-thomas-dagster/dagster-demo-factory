---
action: rebuild-demo
requested: 2026-09-03
---

Previous build faked integration surfaces via a home-made component. Use the REAL component for every external system named in the brief — search the registry and the native integrations. Subclass and mock the I/O seam where credentials are missing, never substitute. Report the system-to-component-ID mapping.
