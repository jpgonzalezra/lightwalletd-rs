#!/usr/bin/env python3
"""Check the English prose in this repo against the conventions in CONTRIBUTING.md.

Regex only, no dependencies. It catches the mechanical tells: em dashes, the AI
vocabulary, wordy phrases that have a one-word equivalent, and the connectives used
as paragraph glue. It deliberately says nothing about sentence length, contractions
or modal verbs, which this project varies on purpose.

Markdown: fenced blocks, code spans, URLs and link targets are never read. Link text
and a lone em dash in a table cell are exempt from the em-dash rule only, since both
quote something written elsewhere. Rust: only doc comments are read, `///` and `//!`
and the `/** */` and `/*! */` block forms, each recognised at the start of a line.

Masking runs over a whole paragraph rather than a line, because a code span or a link
may wrap. Spaces replace the masked text so line and column numbers stay true.

Put `prose-lint: allow` anywhere on a line to skip that line.

Usage:
  python3 scripts/prose-lint.py                 # every tracked file
  python3 scripts/prose-lint.py README.md ...   # only these
  python3 scripts/prose-lint.py --self-test
"""

import re
import subprocess
import sys
from pathlib import Path

ALLOW_MARKER = "prose-lint: allow"

# (id, pattern, advice). Everything is case-insensitive.
RULES = [
    # An en dash between numbers is a range ("blocks 1–64", "60k–140k") and stays.
    # Only a dash spaced like punctuation, or any em dash, is the prose habit.
    (
        "em-dash",
        r"—|(?<=\s)–|–(?=\s)",
        "use a colon, parentheses, or two sentences",
    ),
    (
        "ai-vocab",
        r"\b(?:delv(?:e|es|ed|ing)|seamless(?:ly)?|robust(?:ly|ness)?"
        r"|comprehensive(?:ly)?|leverag(?:e|es|ed|ing)|utiliz(?:e|es|ed|ing)"
        r"|crucial(?:ly)?|pivotal|foster(?:s|ed|ing)?|streamlin(?:e|es|ed|ing)"
        r"|myriad|plethora|effortless(?:ly)?|blazingly|performant|simply"
        r"|powerful|facilitat(?:e|es|ed|ing)|state-of-the-art)\b",
        "use the plain word, or drop the sentence",
    ),
    (
        "ai-frame",
        r"it'?s important to (?:note|remember)|it is important to"
        r"|it is worth noting|\bnote that\b|in today'?s (?:landscape|world)"
        r"|\bserves as\b|\bacts as\b|\bplays an? (?:\w+ )?role\b"
        r"|\baims to\b|\bdesigned to\b|\bensure that\b"
        r"|\bout of the box\b|\bunder the hood\b|\bgracefully handles\b",
        "state the fact directly instead",
    ),
    (
        "wordy",
        r"\bin order to\b|\bprior to\b|\bdue to the fact that\b"
        r"|\bin the event that\b|\bwhen it comes to\b|\bat this point in time\b"
        r"|\bin the process of\b",
        "there is a one-word equivalent",
    ),
    # Anchored to the start of a line (a list marker or heading may precede it) and
    # followed by a comma, so "19% slower overall" and similar stay legal.
    (
        "glue",
        r"^[ \t]*(?:[-*+][ \t]+|\d+\.[ \t]+|#+[ \t]+|>[ \t]*)*"
        r"(?:additionally|furthermore|moreover|overall|notably|importantly)[ \t]*,",
        "drop it, or say what the sentence adds",
    ),
    (
        "summary-closer",
        r"\b(?:in summary|in conclusion|to summarize|to sum up|all in all)\b",
        "a paragraph that restates the section gets deleted",
    ),
    (
        "workspace",
        r"\blearning project\b|\ba port of\b|(?<![\w/.-])plans/",
        "internal framing: describe the decision on its own terms",
    ),
    (
        "third-party-product",
        r"\b(?:ywallet|zashi|nighthawk|zecwallet|edge wallet|unstoppable wallet)\b",
        'name clients generically ("Zcash light wallets")',
    ),
]

COMPILED = [(name, re.compile(p, re.I | re.M), advice) for name, p, advice in RULES]

# The em-dash rule reads a more heavily masked paragraph than the rest.
QUOTE_EXEMPT = {"em-dash"}

FENCE = re.compile(r"^[ \t]*(`{3,}|~{3,})([^\n]*)$")
CODE_RUN = re.compile(r"`+")
URL = re.compile(r"<?\b(?:https?|ftp)://[^\s>)\]]+>?")
LINK_TEXT = re.compile(r"\[[^\]]*\]", re.S)
TABLE_CELL_DASH = re.compile(r"(?<=\|)([ \t]*)[—–]([ \t]*)(?=\|)")
RUST_LINE_DOC = re.compile(r"^[ \t]*(?:///(?!/)|//!)")
# Both Rust doc forms are anchored to the start of the line, which is where they are
# written. That keeps a string literal holding `/**` or `///` from reading as a comment,
# at the cost of missing a doc comment opened mid-line.
RUST_BLOCK_DOC = re.compile(r"^[ \t]*/\*[*!]")


def blank(match):
    """Replace a span with spaces, keeping newlines so line numbers stay true."""
    return "".join("\n" if char == "\n" else " " for char in match.group(0))


def blank_span(chars, start, end):
    for position in range(start, end):
        if chars[position] != "\n":
            chars[position] = " "


def mask_code_spans(text):
    """Blank code spans. A run of N backticks closes only on a run of exactly N.

    An unmatched run is literal text in markdown, so it stays visible to the rules
    instead of masking everything after it.
    """
    chars = list(text)
    runs = [(m.start(), m.end()) for m in CODE_RUN.finditer(text)]
    index = 0
    while index < len(runs):
        width = runs[index][1] - runs[index][0]
        closer = next(
            (j for j in range(index + 1, len(runs)) if runs[j][1] - runs[j][0] == width),
            None,
        )
        if closer is None:
            index += 1
            continue
        blank_span(chars, runs[index][1], runs[closer][0])
        index = closer + 1
    return "".join(chars)


def mask_link_targets(text):
    """Blank the destination in `](...)`, counting nested parentheses."""
    chars = list(text)
    position = 0
    while True:
        opening = text.find("](", position)
        if opening == -1:
            return "".join(chars)
        depth = 0
        cursor = opening + 1
        while cursor < len(text):
            char = text[cursor]
            if char == "\\":
                cursor += 2
                continue
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    break
            cursor += 1
        if depth == 0 and cursor < len(text):
            blank_span(chars, opening + 2, cursor)
            position = cursor + 1
        else:
            position = opening + 2


def strip_fences(lines):
    """Blank fenced blocks.

    A fence closes on the same marker, at least as long, and with nothing after it.
    A closing fence takes no info string, so ``` followed by words stays inside.
    """
    out = []
    marker = None
    for raw in lines:
        match = FENCE.match(raw)
        if marker is None and match:
            marker = match.group(1)
            out.append(" " * len(raw))
        elif marker is None:
            out.append(raw)
        else:
            out.append(" " * len(raw))
            closes = (
                match
                and match.group(1)[0] == marker[0]
                and len(match.group(1)) >= len(marker)
                and not match.group(2).strip()
            )
            if closes:
                marker = None
    return out


def rust_doc_only(lines):
    """Blank everything that is not doc-comment text."""
    out = []
    in_block = False
    for raw in lines:
        if in_block:
            end = raw.find("*/")
            if end == -1:
                out.append(raw)
            else:
                out.append(raw[:end] + " " * len(raw[end:]))
                in_block = False
            continue
        line_doc = RUST_LINE_DOC.match(raw)
        if line_doc:
            out.append(" " * line_doc.end() + raw[line_doc.end() :])
            continue
        block_doc = RUST_BLOCK_DOC.match(raw)
        if block_doc:
            end = raw.find("*/", block_doc.end() - 1)
            if end == -1:
                out.append(" " * block_doc.end() + raw[block_doc.end() :])
                in_block = True
            else:
                head = " " * block_doc.end()
                out.append(head + raw[block_doc.end() : end] + " " * len(raw[end:]))
            continue
        out.append(" " * len(raw))
    return out


def mask_paragraphs(lines):
    """Mask each paragraph as one block, so a code span or a link may wrap."""
    loose = list(lines)
    strict = list(lines)
    start = 0
    while start < len(lines):
        if not lines[start].strip():
            start += 1
            continue
        end = start
        while end < len(lines) and lines[end].strip():
            end += 1
        block = mask_code_spans("\n".join(lines[start:end]))
        block = URL.sub(blank, block)
        block = mask_link_targets(block)
        quoted = TABLE_CELL_DASH.sub(blank, LINK_TEXT.sub(blank, block))
        loose[start:end] = block.split("\n")
        strict[start:end] = quoted.split("\n")
        start = end
    return loose, strict


def lint_text(text, is_rust=False):
    """Yield (line_number, column, rule_id, matched_text, advice)."""
    raw_lines = text.splitlines()
    body = rust_doc_only(raw_lines) if is_rust else strip_fences(raw_lines)
    loose, strict = mask_paragraphs(body)
    findings = []
    for number, raw in enumerate(raw_lines, start=1):
        if ALLOW_MARKER in raw:
            continue
        for name, pattern, advice in COMPILED:
            source = strict if name in QUOTE_EXEMPT else loose
            for match in pattern.finditer(source[number - 1]):
                findings.append(
                    (number, match.start() + 1, name, match.group(0).strip(), advice)
                )
    return sorted(findings)


def tracked(pathspec):
    out = subprocess.run(
        ["git", "ls-files", "-z", *pathspec],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [p for p in out.split("\0") if p]


def default_targets():
    # CHANGELOG.md is a record of past releases, so it is left as written.
    docs = [p for p in tracked(["*.md"]) if Path(p).name != "CHANGELOG.md"]
    return sorted(docs + tracked(["src/*.rs"]))


def run(paths):
    total = 0
    for path in paths:
        try:
            text = Path(path).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as error:
            print(f"{path}: cannot read: {error}", file=sys.stderr)
            return 2
        for lineno, col, name, matched, advice in lint_text(
            text, is_rust=path.endswith(".rs")
        ):
            print(f"{path}:{lineno}:{col}: {name}: {matched!r}: {advice}")
            total += 1
    if total:
        plural = "" if total == 1 else "s"
        print(f"\n{total} finding{plural}.", file=sys.stderr)
        print(
            f"Rewrite the line, or add `{ALLOW_MARKER}` to it when the wording is "
            "quoted from somewhere else.",
            file=sys.stderr,
        )
        return 1
    print(f"prose-lint: {len(paths)} files, no findings.")
    return 0


CASES = [
    # (text, is_rust, expected rule ids)
    ("The retry path is robust.", False, ["ai-vocab"]),
    ("Note that zebra rejects the batch.", False, ["ai-frame"]),
    ("Additionally, the parse ran on the ingest task.", False, ["glue"]),
    ("- Furthermore, the cache truncates.", False, ["glue"]),
    ("A sync is ~19% slower overall (1h 38m).", False, []),
    ("The window acts as a ceiling on concurrency.", False, ["ai-frame"]),
    ("Run it in order to warm the cache.", False, ["wordy"]),
    ("In summary, the cache wins.", False, ["summary-closer"]),
    ("This is a port of the Go server.", False, ["workspace"]),
    ("`--rpc-url` is the host:port of the indexer.", False, []),
    ("See plans/2026-06-12-implementation.md.", False, ["workspace"]),
    ("Tested against Zashi.", False, ["third-party-product"]),
    # A lone em dash in a table cell means "no default" and is not prose.
    ("| `--rpc-url` | — | full JSON-RPC URL |", False, []),
    ("Blocks 20,000–31,999 at 60k–140k blocks/s, p99 33–55 ms.", False, []),
    ("The cache – which truncates on reorg – wins.", False, ["em-dash", "em-dash"]),
    ("Plaintext—do not use in production.", False, ["em-dash"]),
    # Link text quotes an official ZIP title; the em dash after it does not.
    (
        "- **[ZIP-307 — Light Client Protocol](https://zips.z.cash/zip-0307)** — defines",
        False,
        ["em-dash"],
    ),
    ("The `robust` flag is unset.", False, []),
    # A quoted log line that wraps keeps its own punctuation.
    ('the warn is `"plaintext (x) — do\nnot use"`, as expected.', False, []),
    ("```\nleverage the robust API\n```", False, []),
    ("Use the robust path.  <!-- prose-lint: allow -->", False, []),
    ("https://example.com/simply-a-robust-path", False, []),
    # A run of two backticks closes only on another run of two.
    ("The value is ``robust mode``.", False, []),
    ("A ``span with ` inside`` and robust prose.", False, ["ai-vocab"]),
    # An unmatched backtick is literal text, so what follows is still prose.
    ("Use the ` flag.\nThe cache is robust.", False, ["ai-vocab"]),
    # A shorter fence cannot close a longer one.
    ("````text\nrobust\n```\nleverage\n````\nseamless", False, ["ai-vocab"]),
    # A closing fence takes no info string, so this one does not close anything.
    ("```text\ncode\n``` not a closing fence\nrobust\n```", False, []),
    # Link destinations may nest parentheses, and may escape one.
    ("See [spec](guide(foo)robust).", False, []),
    ("See [spec](guide\\)robust).", False, []),
    ("/// Leverage the cache.", True, ["ai-vocab"]),
    ("//! Leverage the crate.", True, ["ai-vocab"]),
    ("/** A robust cache. */", True, ["ai-vocab"]),
    ("/*! Leverage the crate. */", True, ["ai-vocab"]),
    ("/**\n * A robust cache.\n */", True, ["ai-vocab"]),
    ("/* leverage the cache */", True, []),
    ("//// leverage the cache", True, []),
    ("// leverage the cache.", True, []),
    ('let name = "leverage";', True, []),
    ('const EXAMPLE: &str = "/** robust */";', True, []),
    ('    let doc = "/// robust";', True, []),
]


def self_test():
    failures = 0
    for text, is_rust, expected in CASES:
        got = [name for _, _, name, _, _ in lint_text(text, is_rust=is_rust)]
        if got != expected:
            failures += 1
            print(f"FAIL {text!r}\n  expected {expected}\n  got      {got}")
    if failures:
        print(f"\n{failures} of {len(CASES)} cases failed.")
        return 1
    print(f"self-test: {len(CASES)} cases passed.")
    return 0


def main(argv):
    if "--self-test" in argv:
        return self_test()
    paths = [a for a in argv if not a.startswith("-")] or default_targets()
    return run(paths)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
