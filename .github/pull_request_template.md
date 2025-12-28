# Title
Provide Title above, ensure it summarizes the work in the PR. Example PR titles templates:
* feature/ (or build/): describe new functionality
* hotfix/: describe issue fix for immediate release
* bugfix/ (or fix/): describe issue fix, not necessary for immediate release
* docs/: adding or updating documentation
</em>

## Description & motivation:
<em>
High level description your PR, and why you're making it. Is this linked to slack thread, Monday board, open
issue, a continuation to a previous PR? Link it here if relevant (use the "#" symbol for issues/PRs). This section should highlight any breaking changes.
</em>

## Merge Priority:
<em> This checklist helps the reviewers understand the level of priority for merging this PR.</em>

- Low: A week or more.
- Medium: Within 3 days or less.
- High: As soon as possible.

<em>If High Priority, explain why.</em>

## Changes to existing models:
<em>
Include this section if you are changing any existing files or creating breaking changes to existing files. Label the model name and describe the logic behind the changes made, try to be very descriptive here. For example:

- `stg_model` : Describe any changes made to `stg_model` and why the changes where made.
- `src_staging` : Describe any changes made to `src_staging` and why the changes where made.
</em>

## New models created:
<em>
Include this section if you are creating any new files. Label the model name and describe the logic behind the changes made, try to be very descriptive here. For example:

- `stg_new_model` : Describe the purpose of `stg_new_model` and the logic behind creating the model.
- `src_new_staging` : Describe the purpose of `src_new_staging`.
</em>

## Tests and QC done:
<em>
Describe any process that confirms that the files do what is expected, include screenshots if relevant. For example:

- Analyst replication confirmed that updates to `stg_model` new counts were correct.
- Executed a dbt project run and ensured it was successful.
</em>

## <em> Future ToDos & Questions:</em>
<em>
[Optional] Include any future steps and questions related to this PR.
</em>