Proyecto de ETLs del Data source de github

%md
###Staging Layer

#### Transformación de los datos a un nivel inicial

El dataset inicial o primario esta compuesto por una cantidad de archivos JSON que llegan a generar un peso de 37.8 GB. Estos archivos JSON contiene la estructura de los **PullRequestEvent**; dicha estuctura mantiene multiples niveles de objetos, entre estos podemos encontrar a nivel de Struct u Array, los cuales es necesario tratarlos para poder dejarlos a un solo nivel, donde es más factible la extracción de información. 

Para ello se tiene la siguiente estructura JSON inicial, la cual se desgloza de manera general debido a su tamaño, es decir a un nivel 3

```java
root
 |-- actor: struct (nullable = true)
 |    |-- avatar_url: string (nullable = true)
 |    |-- bio: string (nullable = true)
 |    |-- blog: string (nullable = true)
 |    |-- company: string (nullable = true)
 |    |-- created_at: string (nullable = true)
 |    |-- display_login: string (nullable = true)
 |    |-- email: string (nullable = true)
 |    |-- events_url: string (nullable = true)
 |    |-- followers: long (nullable = true)
 |    |-- followers_url: string (nullable = true)
 |    |-- following: long (nullable = true)
 |    |-- following_url: string (nullable = true)
 |    |-- gists_url: string (nullable = true)
 |    |-- gravatar_id: string (nullable = true)
 |    |-- hireable: boolean (nullable = true)
 |    |-- html_url: string (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- location: string (nullable = true)
 |    |-- login: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- node_id: string (nullable = true)
 |    |-- organizations_url: string (nullable = true)
 |    |-- public_gists: long (nullable = true)
 |    |-- public_repos: long (nullable = true)
 |    |-- received_events_url: string (nullable = true)
 |    |-- repos_url: string (nullable = true)
 |    |-- site_admin: boolean (nullable = true)
 |    |-- starred_url: string (nullable = true)
 |    |-- subscriptions_url: string (nullable = true)
 |    |-- twitter_username: string (nullable = true)
 |    |-- type: string (nullable = true)
 |    |-- updated_at: string (nullable = true)
 |    |-- url: string (nullable = true)
 |-- created_at: string (nullable = true)
 |-- id: string (nullable = true)
 |-- org: struct (nullable = true)
 |    |-- avatar_url: string (nullable = true)
 |    |-- blog: string (nullable = true)
 |    |-- company: string (nullable = true)
 |    |-- created_at: string (nullable = true)
 |    |-- description: string (nullable = true)
 |    |-- email: string (nullable = true)
 |    |-- events_url: string (nullable = true)
 |    |-- followers: long (nullable = true)
 |    |-- following: long (nullable = true)
 |    |-- gravatar_id: string (nullable = true)
 |    |-- has_organization_projects: boolean (nullable = true)
 |    |-- has_repository_projects: boolean (nullable = true)
 |    |-- hooks_url: string (nullable = true)
 |    |-- html_url: string (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- is_verified: boolean (nullable = true)
 |    |-- issues_url: string (nullable = true)
 |    |-- location: string (nullable = true)
 |    |-- login: string (nullable = true)
 |    |-- members_url: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- node_id: string (nullable = true)
 |    |-- public_gists: long (nullable = true)
 |    |-- public_members_url: string (nullable = true)
 |    |-- public_repos: long (nullable = true)
 |    |-- repos_url: string (nullable = true)
 |    |-- twitter_username: string (nullable = true)
 |    |-- type: string (nullable = true)
 |    |-- updated_at: string (nullable = true)
 |    |-- url: string (nullable = true)
 |-- payload: struct (nullable = true)
 |    |-- action: string (nullable = true)
 |    |-- number: long (nullable = true)
 |    |-- pull_request: struct (nullable = true)
 |    |    |-- _links: struct (nullable = true)
 |    |    |-- additions: long (nullable = true)
 |    |    |-- assignee: struct (nullable = true)
 |    |    |-- assignees: array (nullable = true)
 |    |    |-- author_association: string (nullable = true)
 |    |    |-- base: struct (nullable = true)
 |    |    |-- body: string (nullable = true)
 |    |    |-- changed_files: long (nullable = true)
 |    |    |-- closed_at: string (nullable = true)
 |    |    |-- comments: long (nullable = true)
 |    |    |-- comments_url: string (nullable = true)
 |    |    |-- commits: long (nullable = true)
 |    |    |-- commits_list: array (nullable = true)
 |    |    |-- commits_url: string (nullable = true)
 |    |    |-- created_at: string (nullable = true)
 |    |    |-- deletions: long (nullable = true)
 |    |    |-- diff_url: string (nullable = true)
 |    |    |-- head: struct (nullable = true)
 |    |    |-- html_url: string (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- issue_url: string (nullable = true)
 |    |    |-- labels: array (nullable = true)
 |    |    |-- locked: boolean (nullable = true)
 |    |    |-- maintainer_can_modify: boolean (nullable = true)
 |    |    |-- merge_commit_sha: string (nullable = true)
 |    |    |-- mergeable: boolean (nullable = true)
 |    |    |-- mergeable_state: string (nullable = true)
 |    |    |-- merged: boolean (nullable = true)
 |    |    |-- merged_at: string (nullable = true)
 |    |    |-- merged_by: struct (nullable = true)
 |    |    |-- milestone: struct (nullable = true)
 |    |    |-- node_id: string (nullable = true)
 |    |    |-- number: long (nullable = true)
 |    |    |-- patch_url: string (nullable = true)
 |    |    |-- rebaseable: boolean (nullable = true)
 |    |    |-- requested_reviewers: array (nullable = true)
 |    |    |-- requested_teams: array (nullable = true)
 |    |    |-- review_comment_url: string (nullable = true)
 |    |    |-- review_comments: long (nullable = true)
 |    |    |-- review_comments_url: string (nullable = true)
 |    |    |-- state: string (nullable = true)
 |    |    |-- statuses_url: string (nullable = true)
 |    |    |-- title: string (nullable = true)
 |    |    |-- updated_at: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |    |    |-- user: struct (nullable = true)
 |-- public: boolean (nullable = true)
 |-- repo: struct (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- url: string (nullable = true)
 |-- type: string (nullable = true)

```

###Requerimiento analítico 
Se debe obtener una estructura de un solo nivel, donde los datos puedan ser accedidos de una manera más facil y rapida, en comparacion con una estructura de arbol, esto con el objetivo de mejorar el performance y cumplir el proposito de la capa intermedia staging-layer, por lo tanto tomar en cuenta:
1. Aplanar a nivel más granular los datos que correspondan con Usuarios
2. Aplanar a un nivel más granular los datos que correspodan con PullRequest
3. Aplanar a nivel más granular los datos que correspondan con Commits

###Muestra del Schema con una estrcutura a nivel uno

**User**
```java
root
 |-- id: long (nullable = true)
 |-- login: string (nullable = true)
 |-- node_id: string (nullable = true)
 |-- avatar_url: string (nullable = true)
 |-- gravatar_id: string (nullable = true)
 |-- url: string (nullable = true)
 |-- html_url: string (nullable = true)
 |-- followers_url: string (nullable = true)
 |-- following_url: string (nullable = true)
 |-- gists_url: string (nullable = true)
 |-- starred_url: string (nullable = true)
 |-- subscriptions_url: string (nullable = true)
 |-- organizations_url: string (nullable = true)
 |-- repos_url: string (nullable = true)
 |-- events_url: string (nullable = true)
 |-- received_events_url: string (nullable = true)
 |-- type: string (nullable = true)
 |-- site_admin: boolean (nullable = true)
 |-- name: string (nullable = true)
 |-- company: string (nullable = true)
 |-- blog: string (nullable = true)
 |-- location: string (nullable = true)
 |-- email: string (nullable = true)
 |-- hireable: boolean (nullable = true)
 |-- bio: string (nullable = true)
 |-- twitter_username: string (nullable = true)
 |-- public_repos: long (nullable = true)
 |-- public_gists: long (nullable = true)
 |-- followers: long (nullable = true)
 |-- following: long (nullable = true)
 |-- created_at: string (nullable = true)
 |-- updated_at: string (nullable = true)
```

**PullRequest y commits**
```java
root
 |-- id: string (nullable = true)
 |-- type: string (nullable = true)
 |-- actor_id: long (nullable = true)
 |-- org_id: long (nullable = true)
 |-- repo_id: long (nullable = true)
 |-- repo_name: string (nullable = true)
 |-- repo_url: string (nullable = true)
 |-- public: boolean (nullable = true)
 |-- created_at: string (nullable = true)
 |-- payload_action: string (nullable = true)
 |-- payload_number: long (nullable = true)
 |-- pull_request_url: string (nullable = true)
 |-- pull_request_id: long (nullable = true)
 |-- pull_request_html_url: string (nullable = true)
 |-- pull_request_diff_url: string (nullable = true)
 |-- pull_request_patch_url: string (nullable = true)
 |-- pull_request_issue_url: string (nullable = true)
 |-- pull_request_number: long (nullable = true)
 |-- pull_request_state: string (nullable = true)
 |-- pull_request_locked: boolean (nullable = true)
 |-- pull_request_title: string (nullable = true)
 |-- pull_request_user_id: long (nullable = true)
 |-- pull_request_body: string (nullable = true)
 |-- pull_request_created_at: string (nullable = true)
 |-- pull_request_updated_at: string (nullable = true)
 |-- pull_request_closed_at: string (nullable = true)
 |-- pull_request_merged_at: string (nullable = true)
 |-- pull_request_merge_commit_sha: string (nullable = true)
 |-- pull_request_assignee_id: long (nullable = true)
 |-- pull_request_milestone: struct (nullable = true)
 |    |-- closed_at: string (nullable = true)
 |    |-- closed_issues: long (nullable = true)
 |    |-- created_at: string (nullable = true)
 |    |-- creator: struct (nullable = true)
 |    |    |-- avatar_url: string (nullable = true)
 |    |    |-- events_url: string (nullable = true)
 |    |    |-- followers_url: string (nullable = true)
 |    |    |-- following_url: string (nullable = true)
 |    |    |-- gists_url: string (nullable = true)
 |    |    |-- gravatar_id: string (nullable = true)
 |    |    |-- html_url: string (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- login: string (nullable = true)
 |    |    |-- node_id: string (nullable = true)
 |    |    |-- organizations_url: string (nullable = true)
 |    |    |-- received_events_url: string (nullable = true)
 |    |    |-- repos_url: string (nullable = true)
 |    |    |-- site_admin: boolean (nullable = true)
 |    |    |-- starred_url: string (nullable = true)
 |    |    |-- subscriptions_url: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |    |-- description: string (nullable = true)
 |    |-- due_on: string (nullable = true)
 |    |-- html_url: string (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- labels_url: string (nullable = true)
 |    |-- node_id: string (nullable = true)
 |    |-- number: long (nullable = true)
 |    |-- open_issues: long (nullable = true)
 |    |-- state: string (nullable = true)
 |    |-- title: string (nullable = true)
 |    |-- updated_at: string (nullable = true)
 |    |-- url: string (nullable = true)
 |-- pull_request_commits_url: string (nullable = true)
 |-- pull_request_review_comments_url: string (nullable = true)
 |-- pull_request_review_comment_url: string (nullable = true)
 |-- pull_request_comments_url: string (nullable = true)
 |-- pull_request_statuses_url: string (nullable = true)
 |-- pull_request_head_sha: string (nullable = true)
 |-- pull_request_base_sha: string (nullable = true)
 |-- pull_request_head_repo: struct (nullable = true)
 |    |-- archive_url: string (nullable = true)
 |    |-- archived: boolean (nullable = true)
 |    |-- assignees_url: string (nullable = true)
 |    |-- blobs_url: string (nullable = true)
 |    |-- branches_url: string (nullable = true)
 |    |-- clone_url: string (nullable = true)
 |    |-- collaborators_url: string (nullable = true)
 |    |-- comments_url: string (nullable = true)
 |    |-- commits_url: string (nullable = true)
 |    |-- compare_url: string (nullable = true)
 |    |-- contents_url: string (nullable = true)
 |    |-- contributors_url: string (nullable = true)
 |    |-- created_at: string (nullable = true)
 |    |-- default_branch: string (nullable = true)
 |    |-- deployments_url: string (nullable = true)
 |    |-- description: string (nullable = true)
 |    |-- downloads_url: string (nullable = true)
 |    |-- events_url: string (nullable = true)
 |    |-- fork: boolean (nullable = true)
 |    |-- forks: long (nullable = true)
 |    |-- forks_count: long (nullable = true)
 |    |-- forks_url: string (nullable = true)
 |    |-- full_name: string (nullable = true)
 |    |-- git_commits_url: string (nullable = true)
 |    |-- git_refs_url: string (nullable = true)
 |    |-- git_tags_url: string (nullable = true)
 |    |-- git_url: string (nullable = true)
 |    |-- has_downloads: boolean (nullable = true)
 |    |-- has_issues: boolean (nullable = true)
 |    |-- has_pages: boolean (nullable = true)
 |    |-- has_projects: boolean (nullable = true)
 |    |-- has_wiki: boolean (nullable = true)
 |    |-- homepage: string (nullable = true)
 |    |-- hooks_url: string (nullable = true)
 |    |-- html_url: string (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- issue_comment_url: string (nullable = true)
 |    |-- issue_events_url: string (nullable = true)
 |    |-- issues_url: string (nullable = true)
 |    |-- keys_url: string (nullable = true)
 |    |-- labels_url: string (nullable = true)
 |    |-- language: string (nullable = true)
 |    |-- languages_url: string (nullable = true)
 |    |-- license: struct (nullable = true)
 |    |    |-- key: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- node_id: string (nullable = true)
 |    |    |-- spdx_id: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |    |-- merges_url: string (nullable = true)
 |    |-- milestones_url: string (nullable = true)
 |    |-- mirror_url: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- node_id: string (nullable = true)
 |    |-- notifications_url: string (nullable = true)
 |    |-- open_issues: long (nullable = true)
 |    |-- open_issues_count: long (nullable = true)
 |    |-- owner: struct (nullable = true)
 |    |    |-- avatar_url: string (nullable = true)
 |    |    |-- events_url: string (nullable = true)
 |    |    |-- followers_url: string (nullable = true)
 |    |    |-- following_url: string (nullable = true)
 |    |    |-- gists_url: string (nullable = true)
 |    |    |-- gravatar_id: string (nullable = true)
 |    |    |-- html_url: string (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- login: string (nullable = true)
 |    |    |-- node_id: string (nullable = true)
 |    |    |-- organizations_url: string (nullable = true)
 |    |    |-- received_events_url: string (nullable = true)
 |    |    |-- repos_url: string (nullable = true)
 |    |    |-- site_admin: boolean (nullable = true)
 |    |    |-- starred_url: string (nullable = true)
 |    |    |-- subscriptions_url: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |    |-- private: boolean (nullable = true)
 |    |-- pulls_url: string (nullable = true)
 |    |-- pushed_at: string (nullable = true)
 |    |-- releases_url: string (nullable = true)
 |    |-- size: long (nullable = true)
 |    |-- ssh_url: string (nullable = true)
 |    |-- stargazers_count: long (nullable = true)
 |    |-- stargazers_url: string (nullable = true)
 |    |-- statuses_url: string (nullable = true)
 |    |-- subscribers_url: string (nullable = true)
 |    |-- subscription_url: string (nullable = true)
 |    |-- svn_url: string (nullable = true)
 |    |-- tags_url: string (nullable = true)
 |    |-- teams_url: string (nullable = true)
 |    |-- trees_url: string (nullable = true)
 |    |-- updated_at: string (nullable = true)
 |    |-- url: string (nullable = true)
 |    |-- watchers: long (nullable = true)
 |    |-- watchers_count: long (nullable = true)
 |-- pull_request_base_repo: struct (nullable = true)
 |    |-- archive_url: string (nullable = true)
 |    |-- archived: boolean (nullable = true)
 |    |-- assignees_url: string (nullable = true)
 |    |-- blobs_url: string (nullable = true)
 |    |-- branches_url: string (nullable = true)
 |    |-- clone_url: string (nullable = true)
 |    |-- collaborators_url: string (nullable = true)
 |    |-- comments_url: string (nullable = true)
 |    |-- commits_url: string (nullable = true)
 |    |-- compare_url: string (nullable = true)
 |    |-- contents_url: string (nullable = true)
 |    |-- contributors_url: string (nullable = true)
 |    |-- created_at: string (nullable = true)
 |    |-- default_branch: string (nullable = true)
 |    |-- deployments_url: string (nullable = true)
 |    |-- description: string (nullable = true)
 |    |-- downloads_url: string (nullable = true)
 |    |-- events_url: string (nullable = true)
 |    |-- fork: boolean (nullable = true)
 |    |-- forks: long (nullable = true)
 |    |-- forks_count: long (nullable = true)
 |    |-- forks_url: string (nullable = true)
 |    |-- full_name: string (nullable = true)
 |    |-- git_commits_url: string (nullable = true)
 |    |-- git_refs_url: string (nullable = true)
 |    |-- git_tags_url: string (nullable = true)
 |    |-- git_url: string (nullable = true)
 |    |-- has_downloads: boolean (nullable = true)
 |    |-- has_issues: boolean (nullable = true)
 |    |-- has_pages: boolean (nullable = true)
 |    |-- has_projects: boolean (nullable = true)
 |    |-- has_wiki: boolean (nullable = true)
 |    |-- homepage: string (nullable = true)
 |    |-- hooks_url: string (nullable = true)
 |    |-- html_url: string (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- issue_comment_url: string (nullable = true)
 |    |-- issue_events_url: string (nullable = true)
 |    |-- issues_url: string (nullable = true)
 |    |-- keys_url: string (nullable = true)
 |    |-- labels_url: string (nullable = true)
 |    |-- language: string (nullable = true)
 |    |-- languages_url: string (nullable = true)
 |    |-- license: struct (nullable = true)
 |    |    |-- key: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- node_id: string (nullable = true)
 |    |    |-- spdx_id: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |    |-- merges_url: string (nullable = true)
 |    |-- milestones_url: string (nullable = true)
 |    |-- mirror_url: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- node_id: string (nullable = true)
 |    |-- notifications_url: string (nullable = true)
 |    |-- open_issues: long (nullable = true)
 |    |-- open_issues_count: long (nullable = true)
 |    |-- owner: struct (nullable = true)
 |    |    |-- avatar_url: string (nullable = true)
 |    |    |-- events_url: string (nullable = true)
 |    |    |-- followers_url: string (nullable = true)
 |    |    |-- following_url: string (nullable = true)
 |    |    |-- gists_url: string (nullable = true)
 |    |    |-- gravatar_id: string (nullable = true)
 |    |    |-- html_url: string (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- login: string (nullable = true)
 |    |    |-- node_id: string (nullable = true)
 |    |    |-- organizations_url: string (nullable = true)
 |    |    |-- received_events_url: string (nullable = true)
 |    |    |-- repos_url: string (nullable = true)
 |    |    |-- site_admin: boolean (nullable = true)
 |    |    |-- starred_url: string (nullable = true)
 |    |    |-- subscriptions_url: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |    |-- private: boolean (nullable = true)
 |    |-- pulls_url: string (nullable = true)
 |    |-- pushed_at: string (nullable = true)
 |    |-- releases_url: string (nullable = true)
 |    |-- size: long (nullable = true)
 |    |-- ssh_url: string (nullable = true)
 |    |-- stargazers_count: long (nullable = true)
 |    |-- stargazers_url: string (nullable = true)
 |    |-- statuses_url: string (nullable = true)
 |    |-- subscribers_url: string (nullable = true)
 |    |-- subscription_url: string (nullable = true)
 |    |-- svn_url: string (nullable = true)
 |    |-- tags_url: string (nullable = true)
 |    |-- teams_url: string (nullable = true)
 |    |-- trees_url: string (nullable = true)
 |    |-- updated_at: string (nullable = true)
 |    |-- url: string (nullable = true)
 |    |-- watchers: long (nullable = true)
 |    |-- watchers_count: long (nullable = true)
 |-- pull_request_author_association: string (nullable = true)
 |-- pull_request_merged: boolean (nullable = true)
 |-- pull_request_mergeable: boolean (nullable = true)
 |-- pull_request_rebaseable: boolean (nullable = true)
 |-- pull_request_mergeable_state: string (nullable = true)
 |-- pull_request_merged_by_id: long (nullable = true)
 |-- pull_request_comments: long (nullable = true)
 |-- pull_request_review_comments: long (nullable = true)
 |-- pull_request_maintainer_can_modify: boolean (nullable = true)
 |-- pull_request_commits: long (nullable = true)
 |-- pull_request_additions: long (nullable = true)
 |-- pull_request_deletions: long (nullable = true)
 |-- pull_request_changed_files: long (nullable = true)
 |-- pull_request_head_repo_owner_id: long (nullable = true)
 |-- pull_request_head_ref: string (nullable = true)
 |-- pull_request_head_repo_description: string (nullable = true)
 |-- pull_request_head_repo_language: string (nullable = true)
 |-- pull_request_head_repo_license: struct (nullable = true)
 |    |-- key: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- node_id: string (nullable = true)
 |    |-- spdx_id: string (nullable = true)
 |    |-- url: string (nullable = true)
 |-- pull_request_head_repo_fork: boolean (nullable = true)
 |-- pull_request_head_repo_archived: boolean (nullable = true)
 |-- pull_request_head_repo_private: boolean (nullable = true)
 |-- pull_request_head_repo_size: long (nullable = true)
 |-- pull_request_head_repo_full_name: string (nullable = true)
 |-- pull_request_head_repo_default_branch: string (nullable = true)
 |-- pull_request_head_repo_open_issues: long (nullable = true)
 |-- pull_request_head_repo_forks: long (nullable = true)
 |-- pull_request_head_repo_id: long (nullable = true)
 |-- pull_request_head_repo_stargazers_count: long (nullable = true)
 |-- pull_request_head_repo_watchers_count: long (nullable = true)
 |-- pull_request_head_repo_pushed_at: string (nullable = true)
 |-- pull_request_base_ref: string (nullable = true)
 |-- pull_request_base_repo_description: string (nullable = true)
 |-- pull_request_base_repo_language: string (nullable = true)
 |-- pull_request_base_repo_license: struct (nullable = true)
 |    |-- key: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- node_id: string (nullable = true)
 |    |-- spdx_id: string (nullable = true)
 |    |-- url: string (nullable = true)
 |-- pull_request_base_repo_fork: boolean (nullable = true)
 |-- pull_request_base_repo_archived: boolean (nullable = true)
 |-- pull_request_base_repo_private: boolean (nullable = true)
 |-- pull_request_base_repo_size: long (nullable = true)
 |-- pull_request_base_repo_full_name: string (nullable = true)
 |-- pull_request_base_repo_default_branch: string (nullable = true)
 |-- pull_request_base_repo_open_issues: long (nullable = true)
 |-- pull_request_base_repo_forks: long (nullable = true)
 |-- pull_request_base_repo_id: long (nullable = true)
 |-- pull_request_base_repo_stargazers_count: long (nullable = true)
 |-- pull_request_base_repo_watchers_count: long (nullable = true)
 |-- pull_request_base_repo_pushed_at: string (nullable = true)
 |-- pull_request_requested_reviewer: struct (nullable = true)
 |    |-- avatar_url: string (nullable = true)
 |    |-- events_url: string (nullable = true)
 |    |-- followers_url: string (nullable = true)
 |    |-- following_url: string (nullable = true)
 |    |-- gists_url: string (nullable = true)
 |    |-- gravatar_id: string (nullable = true)
 |    |-- html_url: string (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- login: string (nullable = true)
 |    |-- node_id: string (nullable = true)
 |    |-- organizations_url: string (nullable = true)
 |    |-- received_events_url: string (nullable = true)
 |    |-- repos_url: string (nullable = true)
 |    |-- site_admin: boolean (nullable = true)
 |    |-- starred_url: string (nullable = true)
 |    |-- subscriptions_url: string (nullable = true)
 |    |-- type: string (nullable = true)
 |    |-- url: string (nullable = true)
 |-- pull_request_assignees_item: struct (nullable = true)
 |    |-- avatar_url: string (nullable = true)
 |    |-- events_url: string (nullable = true)
 |    |-- followers_url: string (nullable = true)
 |    |-- following_url: string (nullable = true)
 |    |-- gists_url: string (nullable = true)
 |    |-- gravatar_id: string (nullable = true)
 |    |-- html_url: string (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- login: string (nullable = true)
 |    |-- node_id: string (nullable = true)
 |    |-- organizations_url: string (nullable = true)
 |    |-- received_events_url: string (nullable = true)
 |    |-- repos_url: string (nullable = true)
 |    |-- site_admin: boolean (nullable = true)
 |    |-- starred_url: string (nullable = true)
 |    |-- subscriptions_url: string (nullable = true)
 |    |-- type: string (nullable = true)
 |    |-- url: string (nullable = true)
 |-- create_at_time_temporal: string (nullable = true)
 |-- create_at_date_temporal: string (nullable = true)
 |-- pull_request_commit_sha: string (nullable = true)
 |-- pull_request_commit_node_id: string (nullable = true)
 |-- pull_request_commit_author_id: long (nullable = true)
 |-- pull_request_commit_author_name: string (nullable = true)
 |-- pull_request_commit_author_email: string (nullable = true)
 |-- pull_request_commit_author_date: string (nullable = true)
 |-- pull_request_commit_committer_id: long (nullable = true)
 |-- pull_request_commit_committer_name: string (nullable = true)
 |-- pull_request_commit_committer_email: string (nullable = true)
 |-- pull_request_commit_committer_date: string (nullable = true)
 |-- pull_request_commit_message: string (nullable = true)
 |-- pull_request_commit_tree_sha: string (nullable = true)
 |-- pull_request_commit_tree_url: string (nullable = true)
 |-- pull_request_commit_url: string (nullable = true)
 |-- pull_request_commit_comment_count: long (nullable = true)
 |-- pull_request_commit_verification_verified: boolean (nullable = true)
 |-- pull_request_commit_verification_reason: string (nullable = true)
 |-- pull_request_commit_verification_signature: string (nullable = true)
 |-- pull_request_commit_verification_payload: string (nullable = true)
 |-- pull_request_commit_html_url: string (nullable = true)
 |-- pull_request_commit_comments_url: string (nullable = true)
 |-- pull_request_commit_total_changes: long (nullable = true)
 |-- pull_request_commit_total_additions: long (nullable = true)
 |-- pull_request_commit_total_deletions: long (nullable = true)
 |-- pull_request_commit_file_sha: string (nullable = true)
 |-- pull_request_commit_file_filename: string (nullable = true)
 |-- pull_request_commit_file_status: string (nullable = true)
 |-- pull_request_commit_file_additions: long (nullable = true)
 |-- pull_request_commit_file_deletions: long (nullable = true)
 |-- pull_request_commit_file_changes: long (nullable = true)
 |-- pull_request_commit_file_blob_url: string (nullable = true)
 |-- pull_request_commit_file_raw_url: string (nullable = true)
 |-- pull_request_commit_file_contents_url: string (nullable = true)
 |-- pull_request_commit_file_patch: string (nullable = true)
 |-- pull_request_commit_parent_sha: string (nullable = true)
```

**Importante**

Se ha tratado la forma en que los datos solo sean procesados hasta un cierto nivel, ya que la siguiente etapa ese encargara de aplicar todas las relgas de ngocio necesaria 


%md
###Presentation-layer

#### Reglas de negocio implementadas

El dataset ha tenido un cierto gradod e transformación a nivel de la capa de Stagin-layer, donde solo se tomo en consideración poder obtener la data necesaria para su procesamiento final, sin tener en cuenta el estado de los datos en sí; por lo cual se encuentra en el proceso de tener que aplicar reglas de negocio para asi dar forma a los datos finales los cuales se almacenaran a nivel de las dimensiones. 

|TRANSFORMACIÓN ETL                   |	CAMPO APLICA    |	UBICACIÓN   |  RESULTADO                                            |
|-------------------------------------|-----------------|-------------- |-------------------------------------------------------|
|Valores nulos en llaves foráneas     |org, assignee, merged_by, reviewer, pull_request.commits_url.author         |org, assignee, merged_by, reviewer, pull_request.commits_url.author| id -1 en FK que son nulas|                                                       
| Valor booleano a valor textual      |      public    |      public   |     Public event / Private event                       |
| Valor booleano a valor textual      |      private    |   event_repo   |     Public repository / Private repository                     |
|Valor booleano a valor textual  |	fork  	|event.repo	|Has not been forked / Has been forked|
|Valor booleano a valor textual	|has_issues	|event.repo|	Has issues / Doesn’t have issues|
|Valor booleano a valor textual	|has_downloads	|event.repo|	Has downloads / Doesn’t have downloads|
|Valor booleano a valor textual	|has_wiki|	event.repo|	Has wiki/ Doesn’t have wiki|
|  Valor booleano a valor textual|	has_pages	|event.repo	|Has pages/ Doesn’t have pages|
|Valor booleano a valor textual|	archived	|event.repo|	Is archived / Is not archived|
|Valor booleano a valor textual	|locked	|event.payload.pull_request	|Is locked / Is not locked|
|Valor booleano a valor textual	|merged|	event.payload.pull_request|	Is merged / Is not merged|
|Valor booleano a valor textual|	mergeable	|event.payload.pull_request|	Is mergeable / Is not mergeable / Not available|
|Valor booleano a valor textual|	site_admin	|event.payload.pull_request.user|	User is not side admin / User is site admin|
|Valor booleano a valor textual|	hireable	|event.payload.pull_request.user|	Is hireable / Is not hireable / Not available|
|Exclusión de dato	|gravatar_id	|event.actor, event.org, event.payload.pull_request.user|	Campo no existente|
|Valores nulos en dimensiones a valores textuales|	description|	event.repo	|Description not available|
|Valores nulos en dimensiones a valores textuales	|homepage	|event.repo|	Homepage url not available|
|Valores nulos en dimensiones a valores textuales|	language	|event.repo	|Languge not available|
|Valores nulos en dimensiones a valores textuales|	license|	event.repo	|License not available|
|Valores nulos en dimensiones a valores textuales|	body	|event.payload.pull_request	|No Pull Request body available|
|Valores nulos en dimensiones a valores textuales|	closed_at|	event.payload.pull_request	|Has not been closed|
|Valores nulos en dimensiones a valores textuales|	merged_at|	event.payload.pull_request	|Has not been merged|
|Valores nulos en dimensiones a valores textuales	|name	|event.payload.pull_request.user|	Name not available|
|Valores nulos en dimensiones a valores textuales	|company	|event.payload.pull_request.user	|Company not available|
|String length igual a cero	|blog|	event.payload.pull_request.user	|Blog not available|
|Valores nulos en dimensiones a valores textuales|	location|	event.payload.pull_request.user|	Location not available|
|Valores nulos en dimensiones a valores textuales|	email|	event.payload.pull_request.user|	Email not available|
|Valores nulos en dimensiones a valores textuales|	bio|	event.payload.pull_request.user|	Biography not available|
|Valores nulos en dimensiones a valores textuales|	twitter_username|	event.payload.pull_request.user	|Twitter username not available|
|Exclusión de dato	|avatar_url	|event.actor, event.org, event.payload.pull_request.user|	Campo no existente |
|Exclusión de dato|	url|	event.actor, event.repo	|Campo no existente |
|Exclusión de dato	|html_url	|event.repo, event.payload.pull_request.user, event.payload.pull_request.commits_url. {each url}|	Campo no existente |
|Exclusión de dato|	forks_url|	event.repo	|Campo no existente |
|Exclusión de dato|	keys_url|	event.repo|	Campo no existente |
|Exclusión de dato|	collaborators_url|	event.repo|	Campo no existente |
|Exclusión de dato|	teams_url|	event.repo|	Campo no existente |
|Exclusión de dato|	issue_events_url|	event.repo|	Campo no existente |
|Exclusión de dato|	events_url	|event.repo, event.payload.pull_request.user	|Campo no existente |
|Exclusión de dato|	commits_url|	event.payload.pull_request	|Campo no existente |
|Exclusión de dato|	review_comments_url|	event.payload.pull_request	|Campo no existente |
|Exclusión de dato|	review_comments_url	|event.payload.pull_request	|Campo no existente |
|Exclusión de dato|	comments_url	|event.payload.pull_request, event.payload.pull_request.commits_url. {each url}	|Campo no existente |
|Exclusión de dato|	statuses_url|	event.payload.pull_request	|Campo no existente |
|Exclusión de dato|	_links	|event.payload.pull_request	|Campo no existente |
|Exclusión de dato	|followers_url|	event.payload.pull_request.user	|Campo no existente |
|Exclusión de dato|	following_url|	event.payload.pull_request.user	|Campo no existente |
|Exclusión de dato|	gists_url|	event.payload.pull_request.user|	Campo no existente |
|Exclusión de dato|	starred_url|	event.payload.pull_request.user	|Campo no existente |
|Exclusión de dato|	subscriptions_url	|event.payload.pull_request.user|	Campo no existente |
|Exclusión de dato	|organizations_url	|event.payload.pull_request.user|	Campo no existente |
|Exclusión de dato	|repos_url	|event.payload.pull_request.user|	Campo no existente |
|Exclusión de dato|	public_gists|	event.payload.pull_request.user|	Campo no existente| 
|Exclusión de dato|	node_id|	event.payload.pull_request.commits_url. {each url}	|Campo no existente |
|Exclusión de dato	|blob_url|	event.payload.pull_request.commits_url.files {each object}	|Campo no existente |
|Exclusión de dato|	raw_url	|event.payload.pull_request.commits_url.files {each object}|	Campo no existente |
|Exclusión de dato|	contents_url	|event.payload.pull_request.commits_url.files {each object}	|Campo no existente |


%md
#### Tipos de Dimensiones utilizadas
Se describen las dimensiones que conforman el modelo dimensional, tanto para PullRequest como Archivo 

1. Date Dimension
  *	Describe características de fechas en base a las cuales se podrán realizar análisis
  *	Esta dimensión debe poseer un registro para identificar al valor null en la llave foránea en la que sea referenciada

2. Time Dimension
  *	Describe características de tiempo en base a las cuales podrán realizar análisis
  *	 Esta dimensión debe poseer un registro para identificar al valor null en la llave foránea en la que sea referenciada

3. User Dimension
  *	Describe a cada usuario registrado en la plataforma con actividad en eventos pull request en el periodo considerado
  *	Posee atributos numéricos discretos que no cambian con cada registro en la factable 
  *	Esta dimensión debe poseer un registro para identificar al valor null en la llave foránea en la que sea referenciada
  *	Posee 3 SCD 0 (user_id, created_at y type) y el resto son tipo 1

4. Branch Dimension
  *	Describe a cada una de las ramas conteniendo la información del repositorio a la que pertenece para enriquecer el contexto de la misma
  *	Posee atributos numéricos discretos que no cambian con cada registro en la factable 
  *	Posee múltiples fuentes de datos
  *	Posee 2 SCD 0 (branch_name y repo_id) y el resto son tipo 1

5. Organization Dimension
  *	Describe a cada una de las organizaciones con actividad en eventos de pull requests en el periodo considerado
  *	Posee atributos numéricos discretos que no cambian con cada registro en la factable 
  *	Esta dimensión debe poseer un registro para identificar al valor null en la llave foránea en la que sea referenciada
  *	Para atributos Nullables se debe establecer un valor de “undefined”
  *	Posee 3 SCD 0 (organization_id, created_at y type) y el resto son tipo 1

6. File Dimension
  *	Describe cada uno de los archivos que han tenido una modificación en el periodo de tiempo considerado
  *	Posee 2 SCD 0 (sha y type) y el resto son tipo 1

7. Commit Dimension
  *	Describe a cada commit en el intervalo de tiempo considerado
  *	Posee un valores numéricos aditivos pero que no cambian por cada registro en la factable por lo cual son descriptivos a un commit específico
  *	Posee múltiples fuentes de datos
  *	Para atributos Nullables se debe establecer un valor de “undefined”
  *	Posee 4 SCD 0 (sha, message, pull_request_id y number_pull_request) y el resto son tipo 1

8. Pull Request Dimension
  *	Describe a cada pull request en el intervalo de tiempo considerado
  *	Posee un valor numérico discreto que no cambia por cada evento en la factable
  *	Para atributos Nullables se debe establecer un valor de “undefined”
  *	Posee 2 SCD 0 (pull_request_id y number) y el resto son tipo 1

9. File Changes Fact : Transactional Fact table
  *	Es una Transactional Fact Table 
  *	Posee 4 métricas completamente aditivas: additions (número de líneas agregadas), deletions (número de líneas eliminadas), byte_changes (tamaño en bytes de los cambios) y size (tamaño del archivo). 
  *	Posee dos métricas derivadas: changes (suma de adiciones y eliminaciones) y una para la diferente de horas entre el commit padre y el commit. 
  *	Posee una dimensión degenerada: status


10.	Pull Request Fact: Transactional Fact table
  *	Es una Transactional Fact Table
  *	Posee 6 métricas completamente aditivas: additions (número de líneas agregadas), deletions (número de líneas eliminadas), changed_files (número de archivos modificados), commits (número de commits), comments (número de comentarios), review_comments (número de comentarios revisados). 
  *	Posee una dimensión degenerada: state


%md
#### Dimensiones y campos

**PullRequest Dimension**

|Llave     |	Nombre Campo |	Tipo           |
|----------|-----------------|-----------------|
|PK        |id               |int NOT NULL     |
|NK        |pull_request_id  |int NOT NULL     |
|          |title            |char(100)        |
|          |body             |char(50)         |
|          |locked           |varchar NOT NULL |

**Date Dimension**

|Llave     |	Nombre Campo      |	Tipo            |
|----------|----------------------|-----------------|
|PK        |date_key              |varchar NOT NULL |
|          |date                  |date NOT NULL    |
|          |full_date_description |date NOT NULL    |
|          |day_of_week           |varchar NOT NULL |
|          |calendar_month        |varchar NOT NULL |
|          |calendar_quarter      |varchar NOT NULL |
|          |calendar_year         |int NOT NULL     |
|          |holiday_indicator     |varchar NOT NULL |
|          |week_of_year          |int  NOT NULL    |


**Time Dimension**

|Llave     |	Nombre Campo      |	Tipo            |
|----------|----------------------|-----------------|
|PK        |time_key              |varchar NOT NULL |
|          |time24h               |time    NOT NULL |
|          |hour                  |int     NOT NULL |
|          |minute                |int     NOT NULL |
|          |second                |int     NOT NULL |
|          |hour12                |int     NOT NULL |
|          |time_ampm             |time    NOT NULL |
|          |meridian_indicator    |char(3) NOT NULL |
|          |period                |varchar NOT NULL |

**Organization Dimension**

|Llave     |	Nombre Campo      |	Tipo            |
|----------|----------------------|-----------------|
|PK        |id                    |int     NOT NULL |
|NK        |orgnanization_id      |int     NOT NULL |
|          |login                 |varchar NOT NULL |
|          |email                 |varchar          |
|          |type                  |varchar NOT NULL |
|          |name                  |varchar NOT NULL |
|          |description           |varchar          |
|          |company               |varchar          |
|          |location              |varchar NOT NULL |
|          |is_verified           |varchar NOT NULL |
|          |has_organization_pro  |varchar NOT NULL |
|          |has_repository_projec |varchar NOT NULL |
|          |blog                  |varchar          |
|          |twitter_username      |varchar          |
|          |created_at_date       |date NOT NULL    |
|          |updated_at_date       |date NOT NULL    |
|          |public_repos          |int  NOT NULL    |
|          |followers             |int  NOT NULL    |
|          |following             |int  NOT NULL    |
|          |public_repos_category |varchar NOT NULL |
|          |followers_category    |varchar NOT NULL |
|          |following_cateogry    |varchar NOT NULL |


**User Dimension**

|Llave     |	Nombre Campo      |	Tipo            |
|----------|----------------------|-----------------|
|PK        |id                    |int     NOT NULL |
|NK        |user_id               |int     NOT NULL |
|          |login                 |varchar NOT NULL |
|          |email                 |varchar          |
|          |type                  |varchar NOT NULL |
|          |name                  |varchar NOT NULL |
|          |location              |varchar NOT NULL |
|          |site_admin            |varchar NOT NULL |
|          |hireable              |varchar NOT NULL |
|          |blog                  |varchar          |
|          |twitter_username      |varchar          |
|          |created_at_date       |date NOT NULL    |
|          |updated_at_date       |date NOT NULL    |
|          |public_repos          |int  NOT NULL    |
|          |followers             |int  NOT NULL    |
|          |following             |int  NOT NULL    |
|          |public_repos_category |varchar NOT NULL |
|          |followers_category    |varchar NOT NULL |
|          |following_cateogry    |varchar NOT NULL |


**Branch Dimension**

|Llave     |	Nombre Campo      |	Tipo            |
|----------|----------------------|-----------------|
|PK        |id                    |int     NOT NULL |
|          |branch_name           |varchar NOT NULL |
|          |protected_branch      |varchar NOT NULL |
|          |full_name_repo        |varchar NOT NULL |
|          |description_repo      |varchar NOT NULL |
|          |default_branch_repo   |varchar NOT NULL |
|          |language_repo         |varchar NOT NULL |
|          |license_repo          |varchar          |
|          |is_forked_repo        |varchar NOT NULL |
|          |disabled_repo         |varchar NOT NULL |
|          |archived_repo         |varchar NOT NULL |
|          |private_repo          |varchar NOT NULL |
|          |size_repo             |varchar NOT NULL |
|          |open_issues_repo      |int     NOT NULL |
|          |forks_repo            |int     NOT NULL |
|          |repo_id               |int     NOT NULL |
|          |stargarze_count       |int     NOT NULL |
|          |watchers_count_repo   |int     NOT NULL |


**Asignees Group Bridge***

|Llave     |	Nombre Campo      |	Tipo            |
|----------|----------------------|-----------------|
|PK        |asignees_group_id     |int     NOT NULL |
|FK        |user_dim_id           |int     NOT NULL |


**Reviewers Group Bridge**

|Llave     |	Nombre Campo      |	Tipo            |
|----------|----------------------|-----------------|
|PK        |reviewers_group_id    |int     NOT NULL |
|FK        |user_dim_id           |int     NOT NULL |


**Pull Request Fact Table**

|Llave     |	Nombre Campo      |	Tipo            |
|----------|----------------------|-----------------|
|PK        |id                    |int     NOT NULL |
|FK1       |pull_request_id       |int     NOT NULL |
|FK2       |base_branch           |int     NOT NULL |
|FK3       |head_branch           |int     NOT NULL |
|FK4       |actor_id              |int     NOT NULL |
|FK5       |merged_by_id          |int     NOT NULL |
|FK6       |user_id               |int     NOT NULL |
|FK7       |owner_repo            |int     NOT NULL |
|FK8       |asignee_group_id      |int     NOT NULL |
|FK9       |reviewers_group_id    |int     NOT NULL |
|FK10      |created_at_date       |varchar NOT NULL |
|FK11      |created_at_time       |varchar NOT NULL |
|FK12      |organization_id       |int     NOT NULL |
|DD        |state                 |varchar NOT NULL |
|          |additions             |int     NOT NULL |
|          |deletions             |int     NOT NULL |
|          |changed_files         |int     NOT NULL |
|          |commits               |int     NOT NULL |
|          |comments              |int     NOT NULL |
|          |review_comments       |int     NOT NULL |
|          |updated_at_date       |date    NOT NULL |
|          |closed_at_date        |date             |
|          |merged_at_date        |date             |
|          |updated_at_time       |time    NOT NULL |
|          |closed_at_time        |time             |
|          |merged_at_time        |time             |
|          |merged                |varchar          |
|          |mergeable             |varchar          |
|          |merge_commit_sha      |varchar          |
|          |author_association    |varchar          |


%md
**Commit Dimension**

|Llave     |	Nombre Campo |	Tipo           |
|----------|-----------------|-----------------|
|PK        |id               |int NOT NULL     |
|NK        |sha              |varchar NOT NULL |
|          |message          |varchar NOT NULL |
|          |message_good_pra |varchar NOT NULL |
|          |pull_request_id  |int     NOT NULL |
|          |number_request_id|int     NOT NULL |
|          |title_pullRequest|varchar          |
|          |body_pullRequest |varchar          |
|          |state_pullRequest|varchar          |
|          |locked_pullRequest|varchar NOT NULL|
|          |merged_pullrequest|varchar         |
|          |merged_commit_sha_pullrequest|varchar|
|          |author_association_pullrequest|varchar|
|          |changes           |int NOT NUL     |
|          |additions         |int NOT NUL     |
|          |DELETIONS         |int NOT NUL     |


**File Dimension**

|Llave     |	Nombre Campo |	Tipo           |
|----------|-----------------|-----------------|
|PK        |id               |int NOT NULL     |
|NK        |sha              |varchar NOT NULL |
|          |name             |varchar NOT NULL |
|          |path             |varchar NOT NULL |
|          |encoding         |varchar NOT NULL |
|          |language         |varchar NOT NULL |
|          |type             |varchar NOT NULL |


**File Changes Fact Table**

|Llave     |	Nombre Campo |	Tipo           |
|----------|-----------------|-----------------|
|PK        |id               |int NOT NULL     |
|FK1       |commit_id        |int    NOT NULL  |
|FK2       |branch_id        |int    NOT NULL  |
|FK3       |author_id        |int    NOT NULL  |
|FK4       |commiter_id      |varchar NOT NULL |
|FK5       |owner_repo       |int     NOT NULL |
|FK6       |file_id          |int     NOT NULL |
|          |additions        |int     NOT NULL |
|          |deletions        |int     NOT NULL |
|          |changed_files    |int     NOT NULL |
|          |byte_changes     |int     NOT NULL |
|          |size             |int     NOT NULL |
|DD        |status           |varchar NOT NULL |
|          |commited_at_date |varchar NOT NULL |
|          |commited_at_time |varchar NOT NULL |
|FK7       |organization_id  |int     NOT NULL |
|          |commited_at_date_full_date |date |
|          |commited_at_parent_full_date |date  |
|          |different_time_commit |int |


%md

####Importante

La capa de presentacion-layer toma como base los datos generados de la capa intermedia staging-layer como fuente primaría, para así aplicar las reglas de negocio definidas a nivel de ETL, esto con el fín de poder obtener la información que sera consumida por el aplicativo de Power Bi para realizar la presentación mediante dashboards.


%md

####Componentes usados AWS
####S3
1. A nivel de AWS, se utilizo el aplicativo de S3 donde se definierón una serie de carpetas para poder ir almacenando los datos en base al procesamiento realizado, para tal caso se definierón tres carpetas:
* Raw Layer: Almacena los datos crudos recién extraidos de la fuente de datos.
*	Staging Layer: Capa intermedia donde se encuentra los datos listos para ser transformados dependiendo del fin que éstos tengan.
*	Presentation Layer: Datos transformados que se utilizan para realizar análisis.

2. La partición de raw-layer contiene los datos a procesar


#####IAM
Es un servicio de Amazon a través de Amazon Web Services que permite controlar el acceso a los demás recursos de AWS. Es posible crear usuarios, roles, políticas, etc. El root user; es decir, el administrador principal de los recursos de AWS, es quien inicialmente crea y brinda permisos a diferentes usuarios. Una buena práctica es utilizar el root user solamente para generar los usuarios y tareas de administración. Para el proyecto se utiliza IAM para brindar permisos a cada uno de los integrantes para acceder al bucket de S3.

Aplicativo con el cual se ha gestionado la creación de usuarios, administración de políticas y permisos para que se pudiese acceder al bucket definido en S3 desde aplicativos externos, como lo fue Databricks. 