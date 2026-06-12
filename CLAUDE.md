  # AGENTS.md

  **Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

  ## 1. Think Before Coding

  Before implementing:
  - State assumptions explicitly. If anything is unclear or uncertain, stop and ask; name what's confusing.
  - If multiple interpretations exist, present them; don't pick silently.
  - If a simpler approach exists, say so. Push back when warranted.

  ## 2. Simplicity First

  Minimum code that solves the problem. Nothing speculative.

  - No features beyond what was asked.
  - No abstractions for single-use code.
  - No "flexibility" or "configurability" that wasn't requested.
  - No error handling for impossible scenarios.
  - If you write 200 lines and it could be 50, rewrite it.

  Self-check: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

  ## 3. Surgical Changes

  Touch only what you must. Every changed line should trace directly to the user's request.

  When editing existing code:
  - Don't "improve" adjacent code, comments, or formatting.
  - Don't refactor things that aren't broken.
  - Match existing style, even if you'd do it differently.
  - Remove imports/variables/functions that YOUR changes orphaned. Leave pre-existing dead code alone; mention it instead of deleting it.

  ## 4. Goal-Driven Execution

  Transform tasks into verifiable goals:
  - "Add validation" → "Write tests for invalid inputs, then make them pass"
  - "Fix the bug" → "Write a test that reproduces it, then make it pass"
  - "Refactor X" → "Ensure tests pass before and after"

  For multi-step tasks, state a brief plan:
  1. [Step] → verify: [check]
  2. [Step] → verify: [check]

  Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

  ## Tools & Setup

  - Line length: 88 chars
  - Use context7 MCP or the find-docs skill when looking up library documentation
  - Use rust-skills (the skill) to check the quality of your work

  ## Hard Rules

  - Docstrings (with args, returns, raises) on all public functions, classes, and methods
  - SOLID principles; use design patterns (https://rust-unofficial.github.io/patterns/patterns/index.html) when appropriate
  - No leaky abstractions
  - Early returns/continues over deep nesting of the happy path
  - No magic numbers: use named constants
  - No emoji or unicode emoji substitutes (e.g. checkmarks, crosses) in code or output
  - No secrets in code: use `.env` only; ensure `.env` and test output dirs are in `.gitignore`
  - No logging of sensitive data (passwords, tokens, PII)

  ## Design

  - Max 5 parameters per function (`__init__` excluded)
  - Dependency injection for complex dependencies; classes must be mockable

  ## Code Review

  - Check for leaky abstractions and poor code design
  - Pay special attention to failure modes and data races
  - Check for failure modes in distributed systems (races, lost writes, partial writes)

  ## Testing

  - Mock all external dependencies (APIs, DBs, filesystem)
  - Generate functional tests that exercise behavior end-to-end as a black box

  ## Commits

  - No commented-out code, debug prints, or credentials

  ## Learned User Preferences

  - Inline trivial (1-line) helpers rather than wrapping them; user repeatedly removed `_dt`, `_to_*`, `_neo4j_props`, `_from_neo4j` wrappers in favor of direct calls
- Prefer composition over inheritance
