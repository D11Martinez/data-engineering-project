
 
create table if not exists user_dimension(
 pk_id bigint not null,
 user_id varchar(150) not null,
 login varchar(150) not null, 
 email varchar(150) not null, 
 type varchar(20) not null, 
 site_admin varchar(150) not null,
 name varchar(150) not null, 
 location varchar(200) not null, 
 hirable varchar(50) not null,
 bio varchar(250) not null,
 company varchar(150) not null, 
 blog varchar(250) not null, 
 twitter_username varchar(150) not null, 
 created_at varchar(60) not null, 
 updated_at varchar(60) not null, 
 public_repos varchar(25) not null, 
 followers varchar(25) not null, 
 following varchar(25) not null, 
 followers_category varchar(25) not null, 
 following_category varchar(25 not null, 
 public_repos_category varchar(25) not null, 

 primary key(pk_id))
  
distkey(pk_id)


create table if not exists pull_request_dimension(
 pk_id bigint not null,
 pull_request_id varchar(50) not null,
 "number" varchar(20) not null, 
 title varchar(150) not null, 
 body varchar(250) not null,
 "locked" varchar(50) not null,

 primary key(pk_id))
  
distkey(pk_id)


create table if not exists organization_dimension(
 pk_id bigint not null,
 organization_id varchar(150) not null,
 login varchar(150) not null, 
 email varchar(150) not null, 
 type varchar(20) not null, 
 name varchar(150) not null, 
 description varchar(250) not null, 
 company varchar(150) not null, 
 location varchar(200) not null, 
 is_verified varchar(40) not null, 
 has_organization_projects varchar(40) not null, 
 has_repository_projects varchar(40) not null, 
 blog varchar(250) not null, 
 twitter_username varchar(150) not null, 
 created_at varchar(60) not null, 
 updated_at varchar(60) not null, 
 public_repos varchar(25) not null, 
 followers varchar(25) not null, 
 following varchar(25) not null, 
 followers_category varchar(25) not null, 
 following_category varchar(25 not null, 
 public_repos_category varchar(25) not null, 

 primary key(pk_id))
  
distkey(pk_id)


create table if not exists file_dimension(
 sha varchar(250) not null,
 name varchar(150) not null,
 path varchar(350) not null,
 extension varchar(150) not null,
 full_file_name varchar(350) not null,
 language varchar(100) not null,
 pk_id bigint not null,


 primary key(pk_id))
  
distkey(pk_id)


create table if not exists commit_dimension(
  sha varchar(250) not null,
  message varchar(250) not null,
  message_with_good_practices varchar(60) not null,
  changes varchar(30) not null,
  additions varchar(30) not null,
  deletions varchar(30) not null,
  comment_count varchar(30) not null,
  pull_request_id varchar(50) not null,
  pull_request_number varchar(20) not null,
  pull_request_title varchar(150) not null,
  pull_request_body varchar(250) not null,
  pull_request_state varchar(50) not null,
  pull_request_locked varchar(50) not null,
  pull_request_merged varchar(50) not null,
  pull_request_merge_commit_sha varchar(250) not null,
  pull_request_author_association varchar(50) not null,
  pk_id bigint not null,


 primary key(pk_id))
  
distkey(pk_id)


create table if not exists branch_dimension(
  branch_name varchar(200) ,
  branch_sha varchar(250) ,
  protected_branch varchar(50) ,
  full_name_repo varchar(250) ,
  description_repo varchar(250) ,
  default_branch_repo varchar(100) ,
  language_repo varchar(60) ,
  license_repo varchar(150) ,
  is_forked_repo varchar(50) ,
  archived_repo varchar(50) ,
  private_repo varchar(50) ,
  size_repo varchar(30) ,
  disabled_repo varchar(50) ,
  open_issues_repo varchar(30) ,
  forks_repo varchar(30) ,
  repo_id varchar(50) ,
  stargazer_count_repo varchar(30) ,
  watchers_count_repo varchar()30 ,
  pushed_at varchar(60) ,
  pk_id bigint not null ,

 primary key(pk_id))
  
distkey(pk_id)


create table if not exists assignees_group_bridge(
  asignees_group_id bigint not null,
  user_dim_id bigint not null,


 primary key(asignees_group_id))
  
distkey(asignees_group_id)


create table if not exists reviewers_group_bridge(
  reviewers_group_id bigint not null,
  user_dim_id bigint not null,


 primary key(reviewers_group_id))
  
distkey(reviewers_group_id)



create table if not exists pull_request_fact(
  created_at_date varchar(25) not null,
  created_at_time varchar(25) not null,
  state varchar(40) not null,
  additions bigint not null,
  deletions bigint not null,
  changed_files bigint not null,
  commits bigint not null,
  comments bigint not null,
  review_comments bigint not null,
  updated_at_date varchar(25) not null,
  closed_at_date varchar(25) not null,
  merged_at_date varchar(25) not null,
  updated_at_time varchar(25) not null,
  closed_at_time varchar(25) not null,
  merged_at_time varchar(60) not null,
  merged varchar(50) not null,
  mergeable varchar(50) not null,
  author_association varchar() not null,
  assignee_group_id bigint not null,
  reviewers_group_id bigint not null,
  different_time_pullrequest decimal(8,2) not null,
  pull_request_id bigint not null,
  head_branch bigint not null,
  base_branch bigint not null,
  actor_id bigint not null,
  merged_by_id bigint not null,
  user_id bigint not null,
  owner_repo bigint not null,
  organization_id bigint not null,
  canceled varchar(50) not null,
  pk_id bigint not null,

 primary key(pk_id),
foreign key(assignee_group_id) references assignee_group_bridge(assignees_group_id),
foreign key(reviewers_group_id) references reviewers_group_bridge(reviewers_group_id),
foreign key(pull_request_id) references pull_request_dimension(pk_id),
foreign key(head_branch) references branch_dimension(pk_id),
foreign key(base_branch) references branch_dimension(pk_id),
foreign key(actor_id) references user_dimension(pk_id),
foreign key(merged_by) references user_dimension(pk_id),
foreign key(user_id) references user_dimension(pk_id),
foreign key(owner_repo) references user_dimension(pk_id),
foreign key(organization_id) references organization_dimension(pk_id),
foreign key(created_at_date) references date_dimension(date_key))
 
  
distkey(pk_id)


create table if not exists file_changes_fact(
  commit_id bigint not null,
  file_id bigint not null,
  organization_id bigint not null,
  author_id bigint not null,
  committer_id bigint not null,
  repo_owner_id bigint not null,
  branch_id bigint not null,
  additions bigint not null,
  deletions bigint not null,
  changes bigint not null,
  file_status varchar(50) not null,
  bytes_changes integer not null,
  committed_at_time varchar(25) not null,
  committed_at_date varchar(25) not null,
  committed_at_date_full varchar(60) not null,
  pk_id bigint not null,
  committed_at_parent_date_full varchar(60) not null,
  diferent_time_commit decimal(8,2) not null,

 primary key(pk_id),
 foreign key(commit_id) references commit_dimension(pk_id),
foreign key(file_id) references file_dimension(pk_id),
foreign key(organization_id) references organization_dimension(pk_id),
foreign key(author_id) references user_dimension(pk_id),
foreign key(committer_id) references user_dimension(pk_id),
foreign key(repo_owner) references user_dimension(pk_id),
foreign key(branch_id) references branch_dimension(pk_id),
foreign key(committed_at_date) references date_dimension(date_key))
 
  
distkey(pk_id)

