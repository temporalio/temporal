# Proposing Changes to Cadence

## Introduction

The design process for changes to Cadence is modeled on the [proposal process used by the M3 project](https://github.com/m3db/proposal).

## Process

- [Create an issue](https://github.com/uber/cadence/issues/new) describing the proposal.

- Like any GitHub issue, a proposal issue is followed by an initial discussion about the suggestion. For Proposal issues:

  - The goal of the initial discussion is to reach agreement on the next step: (1) accept, (2) decline, or (3) ask for a design doc.
  - The discussion is expected to be resolved in a timely manner.
  - If the author wants to write a design doc, then they can write one.
  - A lack of agreement means the author should write a design doc.

- If a Proposal issue leads to a design doc:

  - The design doc should be presented as a Google Doc and must follow [the template](https://docs.google.com/document/d/1hpWpy5MB5l8uXfnl23lebhx-dvo79vb1jFUOyAtIHJw/edit?usp=sharing).
  - The design doc should be linked from the opened GitHub issue.
  - The design doc should only allow edit access to authors of the document.
  - Do not create the document from a corporate G Suite account. If you want to edit from a corporate G Suite account then first create the document from a personal Google account and grant edit access to your corporate G Suite account.
  - Comment access should be granted explicitly to [cadence-discussion@googlegroups.com](https://groups.google.com/d/forum/cadence-discussion) so that users viewing the document display under their account names rather than anonymous users.
  - Comment access should also be accessible by the public. Make sure this is the case by clicking "Share" and "Get shareable link" ensuring you select "Anyone with the link can comment".
  
- Once comments and revisions on the design doc wind down, there is a final discussion about the proposal.

  - The goal of the final discussion is to reach agreement on the next step: (1) accept or (2) decline.
  - The discussion is expected to be resolved in a timely manner.

- Once the design doc is agreed upon, the author shall export the contents of the Google Doc as a Markdown document and submit a PR to add it to the proposal repository.

  - Authors can use [gdocs2md](https://github.com/mangini/gdocs2md) to convert the document but any cleanup required must be done by hand.
  - The design doc should be checked in at `docs/design/XYZ-shortname.md`, where `XYZ` is the GitHub issue number and `shortname` is a short name (a few dash-separated words at most).
  - The design doc should be added to the list of existing proposals in `docs/design/index.md`

- The author (and/or other contributors) do the work as described by the "Implementation" section of the proposal.

---

This project is released under the [Apache License, Version 2.0](LICENSE).
