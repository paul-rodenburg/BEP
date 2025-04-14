CREATE TABLE `post` (
  `id` text PRIMARY KEY,
  `author_fullname` text,
  `created_utc` integer,
  `selftext` text,
  `archived` bool,
  `downs` integer,
  `ups` integer,
  `edited` bool,
  `hidden` bool,
  `locked` bool,
  `num_comments` integer,
  `num_crossposts` integer,
  `permalink` text,
  `score` integer,
  `send_replies` bool,
  `spoiler` bool,
  `subreddit` text,
  `thumbnail` text,
  `thumbnail_height` integer,
  `thumbnail_width` integer,
  `title` text,
  `total_awards_received` integer,
  `top_awarded_type` text,
  `pinned` bool,
  `contest_mode` bool
);

CREATE TABLE `author` (
  `author_fullname` text PRIMARY KEY,
  `author` text,
  `author_premium` bool,
  `over_18` bool
);

CREATE TABLE `subreddit` (
  `id` text PRIMARY KEY,
  `name` text,
  `display_name` text,
  `display_name_prefixed` text,
  `title` text,
  `description` text,
  `public_description` text,
  `url` text,
  `subreddit_type` text,
  `lang` text,
  `created_utc` integer,
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
  `retrieved_on` integer,
  `should_archive_posts` bool,
  `should_show_media_in_comments_setting` bool,
  `show_media` bool,
  `show_media_preview` bool,
  `user_flair_enabled_in_sr` bool,
  `user_flair_position` text,
  `user_flair_text` text,
  `user_flair_type` text,
  `user_sr_theme_enabled` bool,
  `videostream_links_count` integer
);

CREATE TABLE `subreddit_metadata` (
  `display_name` text PRIMARY KEY,
  `earliest_comment_at` integer,
  `earliest_post_at` integer,
  `num_comments` integer,
  `num_comments_updated_at` integer,
  `num_posts` integer,
  `num_posts_updated_at` integer
);

CREATE TABLE `subreddit_settings` (
  `display_name` text PRIMARY KEY,
  `accept_followers` bool,
  `accounts_active` integer,
  `accounts_active_is_fuzzed` bool,
  `advertiser_category` text,
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
  `submission_type` text,
  `submit_link_label` text,
  `submit_text` text,
  `submit_text_html` text,
  `submit_text_label` text,
  `suggested_comment_sort` text,
  `notification_level` text
);

CREATE TABLE `subreddit_media` (
  `display_name` text PRIMARY KEY,
  `banner_background_color` text,
  `banner_background_image` text,
  `banner_img` text,
  `banner_size` text,
  `community_icon` text,
  `header_img` text,
  `header_size` text,
  `header_title` text,
  `icon_img` text,
  `icon_size` text,
  `key_color` text,
  `mobile_banner_image` text,
  `primary_color` text
);

CREATE TABLE `subreddit_permissions` (
  `display_name` text PRIMARY KEY,
  `free_form_reports` bool,
  `link_flair_enabled` bool,
  `link_flair_position` text,
  `user_can_flair_in_sr` bool,
  `user_flair_background_color` text,
  `user_flair_css_class` text,
  `user_flair_template_id` text,
  `user_flair_text_color` text
);

CREATE TABLE `subreddit_comment_media` (
  `display_name` text PRIMARY KEY,
  `allowed_media_in_comments` text
);

CREATE TABLE `subreddit_rules` (
  `rule_id` text PRIMARY KEY,
  `subreddit` text,
  `created_utc` integer,
  `description` text,
  `kind` text,
  `priority` integer,
  `short_name` text,
  `violation_reason` text
);

CREATE TABLE `banned` (
  `author_fullname` text,
  `subreddit_id` text,
  `banned_at_utc` integer,
  `banned_by` text,
  PRIMARY KEY (`author_fullname`, `subreddit_id`)
);

CREATE TABLE `comment` (
  `id` text PRIMARY KEY,
  `parent_id` text,
  `body` text,
  `author_fullname` text,
  `created_utc` integer,
  `archived` bool,
  `downs` integer,
  `ups` integer,
  `edited` bool,
  `hidden` bool,
  `locked` bool,
  `permalink` text,
  `removal_reason` text,
  `removed_by` text,
  `score` integer,
  `send_replies` bool,
  `spoiler` bool,
  `subreddit` text,
  `thumbnail` text,
  `thumbnail_height` integer,
  `thumbnail_width` integer,
  `title` text,
  `total_awards_received` integer,
  `top_awarded_type` text,
  `pinned` bool,
  `contest_mode` bool
);

CREATE TABLE `wiki` (
  `path` text PRIMARY KEY,
  `subreddit` text,
  `content` text,
  `retrieved_on` integer,
  `revision_date` date
);

CREATE TABLE `revision_wiki` (
  `path` text PRIMARY KEY,
  `revision_author` text,
  `revision_author_id` text,
  `revision_reason` text
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

ALTER TABLE `wiki` ADD FOREIGN KEY (`subreddit`) REFERENCES `subreddit` (`display_name`);

ALTER TABLE `wiki` ADD FOREIGN KEY (`path`) REFERENCES `revision_wiki` (`path`);

ALTER TABLE `banned` ADD FOREIGN KEY (`subreddit_id`) REFERENCES `subreddit` (`id`);
