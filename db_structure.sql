CREATE TABLE `post` (
  `id` varchar(255) PRIMARY KEY,
  `author_fullname` varchar(255),
  `created_utc` timestamp,
  `selftext` varchar(255), -- 'Body of post'
  `archived` bool,
  `downs` integer,
  `ups` integer,
  `edited` integer,
  `hidden` bool,
  `locked` bool,
  `num_comments` integer,
  `num_crossposts` integer,
  `permalink` varchar(255),
  `score` integer,
  `send_replies` bool,
  `spoiler` bool,
  `subreddit` varchar(255),
  `thumbnail` varchar(255),
  `thumbnail_height` integer,
  `thumbnail_width` integer,
  `title` varchar(255),
  `total_awards_received` integer,
  `top_awarded_type` varchar(255), -- 'Can be NULL'
  `pinned` bool,
  `contest_mode` bool,
  `banned_at_utc` timestamp,
  `banned_by` varchar(255)
);

CREATE TABLE `author` (
  `author_fullname` varchar(255) PRIMARY KEY,
  `author` varchar(255),
  `author_premium` bool,
  `over_18` bool,
  `is_banned` bool -- 'Faster lookup if directly in table'
);

CREATE TABLE `subreddit` (
  `id` varchar(255) PRIMARY KEY,
  `name` varchar(255),
  `display_name` varchar(255),
  `display_name_prefixed` varchar(255),
  `title` varchar(255),
  `description` varchar(255),
  `public_description` varchar(255),
  `url` varchar(255),
  `subreddit_type` varchar(255),
  `lang` varchar(255),
  `created_utc` timestamp,
  `subscribers` integer,
  `over18` bool,
  `allow_discovery` bool,
  `allow_galleries` bool,
  `allow_images` bool,
  `allow_polls` bool,
  `allow_videogifs` bool,
  `allow_videos` bool,
  `allow_predictions` bool,
  `allow_predictions_tournament` bool,
  `allow_talks` bool,
  `is_crosspostable_subreddit` bool,
  `community_reviewed` bool,
  `hide_ads` bool,
  `restrict_commenting` bool,
  `restrict_posting` bool,
  `quarantine` bool,
  `spoilers_enabled` bool,
  `public_traffic` bool,
  `wiki_enabled` bool,
  `retrieved_on` timestamp,
  `should_archive_posts` bool,
  `should_show_media_in_comments_setting` bool,
  `show_media` bool,
  `show_media_preview` bool,
  `user_flair_enabled_in_sr` bool,
  `user_flair_position` varchar(255),
  `user_flair_text` varchar(255),
  `user_flair_type` varchar(255),
  `user_sr_theme_enabled` bool,
  `videostream_links_count` integer
);

CREATE TABLE `subreddit_metadata` (
  `display_name` varchar(255) PRIMARY KEY,
  `earliest_comment_at` timestamp,
  `earliest_post_at` timestamp,
  `num_comments` integer,
  `num_comments_updated_at` timestamp,
  `num_posts` integer,
  `num_posts_updated_at` timestamp
);

CREATE TABLE `subreddit_settings` (
  `display_name` varchar(255) PRIMARY KEY,
  `accept_followers` bool,
  `accounts_active` integer,
  `accounts_active_is_fuzzed` bool,
  `advertiser_category` varchar(255),
  `all_original_content` bool,
  `allow_prediction_contributors` bool,
  `can_assign_link_flair` bool,
  `can_assign_user_flair` bool,
  `collapse_deleted_comments` bool,
  `comment_score_hide_mins` integer,
  `disable_contributor_requests` bool,
  `has_menu_widget` bool,
  `original_content_tag_enabled` bool,
  `prediction_leaderboard_entry_type` integer,
  `submission_type` varchar(255),
  `submit_link_label` varchar(255),
  `submit_text` varchar(255),
  `submit_text_html` varchar(255),
  `submit_text_label` varchar(255),
  `suggested_comment_sort` varchar(255),
  `notification_level` varchar(255)
);

CREATE TABLE `subreddit_media` (
  `display_name` varchar(255) PRIMARY KEY,
  `banner_background_color` varchar(255),
  `banner_background_image` varchar(255),
  `banner_img` varchar(255),
  `banner_size` varchar(255),
  `community_icon` varchar(255),
  `header_img` varchar(255),
  `header_size` varchar(255),
  `header_title` varchar(255),
  `icon_img` varchar(255),
  `icon_size` varchar(255),
  `key_color` varchar(255),
  `mobile_banner_image` varchar(255),
  `primary_color` varchar(255)
);

CREATE TABLE `subreddit_permissions` (
  `display_name` varchar(255) PRIMARY KEY,
  `free_form_reports` bool,
  `link_flair_enabled` bool,
  `link_flair_position` varchar(255),
  `user_can_flair_in_sr` bool,
  `user_flair_background_color` varchar(255),
  `user_flair_css_class` varchar(255),
  `user_flair_template_id` varchar(255),
  `user_flair_text_color` varchar(255)
);

CREATE TABLE `subreddit_comment_media` (
  `display_name` varchar(255),
  `media_type` varchar(255),
  PRIMARY KEY (`display_name`, `media_type`)
);

CREATE TABLE `subreddit_rules` (
  `subreddit` varchar(255) PRIMARY KEY,
  `created_utc` timestamp,
  `description` varchar(255),
  `kind` varchar(255),
  `priority` integer,
  `short_name` varchar(255),
  `violation_reason` varchar(255)
);

CREATE TABLE `banned` (
  `author_fullname` varchar(255) PRIMARY KEY,
  `banned_at_utc` timestamp,
  `banned_by` varchar(255)
);

CREATE TABLE `comment` (
  `id` varchar(255) PRIMARY KEY,
  `parent_id` varchar(255),
  `body` varchar(255),
  `author_fullname` varchar(255),
  `created_utc` timestamp,
  `archived` bool,
  `downs` integer,
  `ups` integer,
  `edited` bool,
  `hidden` bool,
  `locked` bool,
  `permalink` varchar(255),
  `removal_reason` varchar(255), -- 'Can be NULL'
  `removed_by` varchar(255), -- 'Can be NULL'
  `score` integer,
  `send_replies` bool,
  `spoiler` bool,
  `subreddit` varchar(255),
  `thumbnail` varchar(255),
  `thumbnail_height` integer,
  `thumbnail_width` integer,
  `title` varchar(255),
  `total_awards_received` integer,
  `top_awarded_type` varchar(255), -- 'Can be NULL'
  `pinned` bool,
  `contest_mode` bool
);

CREATE TABLE `wiki` (
  `path` varchar(255) PRIMARY KEY,
  `subreddit` varchar(255),
  `content` varchar(255),
  `retrieved_on` timestamp,
  `revision_date` date
);

CREATE TABLE `revision_wiki` (
  `path` varchar(255) PRIMARY KEY,
  `revision_author` varchar(255), -- 'Can be NULL'
  `revision_author_id` varchar(255), -- 'Can be NULL'
  `revision_reason` varchar(255) -- 'Can be null'
);

ALTER TABLE `post` ADD FOREIGN KEY (`author_fullname`) REFERENCES `author` (`author_fullname`);

ALTER TABLE `post` ADD FOREIGN KEY (`subreddit`) REFERENCES `subreddit` (`display_name`);

ALTER TABLE `subreddit_rules` ADD FOREIGN KEY (`subreddit`) REFERENCES `subreddit` (`display_name`);

ALTER TABLE `banned` ADD FOREIGN KEY (`author_fullname`) REFERENCES `author` (`author_fullname`);

ALTER TABLE `comment` ADD FOREIGN KEY (`parent_id`) REFERENCES `post` (`id`);

ALTER TABLE `comment` ADD FOREIGN KEY (`author_fullname`) REFERENCES `author` (`author_fullname`);

ALTER TABLE `subreddit` ADD FOREIGN KEY (`display_name`) REFERENCES `subreddit_metadata` (`display_name`);

ALTER TABLE `subreddit` ADD FOREIGN KEY (`display_name`) REFERENCES `subreddit_settings` (`display_name`);

ALTER TABLE `subreddit` ADD FOREIGN KEY (`display_name`) REFERENCES `subreddit_media` (`display_name`);

ALTER TABLE `subreddit` ADD FOREIGN KEY (`display_name`) REFERENCES `subreddit_permissions` (`display_name`);

CREATE TABLE `subreddit_comment_media_subreddit` (
  `subreddit_comment_media_display_name` varchar,
  `subreddit_display_name` varchar,
  PRIMARY KEY (`subreddit_comment_media_display_name`, `subreddit_display_name`)
);

ALTER TABLE `subreddit_comment_media_subreddit` ADD FOREIGN KEY (`subreddit_comment_media_display_name`) REFERENCES `subreddit_comment_media` (`display_name`);

ALTER TABLE `subreddit_comment_media_subreddit` ADD FOREIGN KEY (`subreddit_display_name`) REFERENCES `subreddit` (`display_name`);

ALTER TABLE `wiki` ADD FOREIGN KEY (`subreddit`) REFERENCES `subreddit` (`display_name`);

ALTER TABLE `wiki` ADD FOREIGN KEY (`path`) REFERENCES `revision_wiki` (`path`);
