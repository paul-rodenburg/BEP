CREATE TABLE `post` (
  `id` TEXT PRIMARY KEY,
  `author_fullname` TEXT,
  `created_utc` integer,
  `selftext` TEXT,
  `archived` bool,
  `downs` integer,
  `ups` integer,
  `edited` bool,
  `hidden` bool,
  `locked` bool,
  `num_comments` integer,
  `num_crossposts` integer,
  `permalink` TEXT,
  `score` integer,
  `send_replies` bool,
  `spoiler` bool,
  `subreddit` TEXT,
  `thumbnail` TEXT,
  `thumbnail_height` integer,
  `thumbnail_width` integer,
  `title` TEXT,
  `total_awards_received` integer,
  `top_awarded_type` TEXT,
  `pinned` bool,
  `contest_mode` bool
);

CREATE TABLE `author` (
  `author_fullname` TEXT PRIMARY KEY,
  `author` TEXT,
  `author_premium` bool,
  `over_18` bool
);

CREATE TABLE `subreddit` (
  `id` TEXT PRIMARY KEY,
  `name` TEXT,
  `display_name` TEXT,
  `display_name_prefixed` TEXT,
  `title` TEXT,
  `description` TEXT,
  `public_description` TEXT,
  `url` TEXT,
  `subreddit_type` TEXT,
  `lang` TEXT,
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
  `user_flair_position` TEXT,
  `user_flair_text` TEXT,
  `user_flair_type` TEXT,
  `user_sr_theme_enabled` bool,
  `videostream_links_count` integer
);

CREATE TABLE `subreddit_metadata` (
  `display_name` TEXT PRIMARY KEY,
  `earliest_comment_at` integer,
  `earliest_post_at` integer,
  `num_comments` integer,
  `num_comments_updated_at` integer,
  `num_posts` integer,
  `num_posts_updated_at` integer
);

CREATE TABLE `subreddit_settings` (
  `display_name` TEXT PRIMARY KEY,
  `accept_followers` bool,
  `accounts_active` integer,
  `accounts_active_is_fuzzed` bool,
  `advertiser_category` TEXT,
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
  `submission_type` TEXT,
  `submit_link_label` TEXT,
  `submit_text` TEXT,
  `submit_text_html` TEXT,
  `submit_text_label` TEXT,
  `suggested_comment_sort` TEXT,
  `notification_level` TEXT
);

CREATE TABLE `subreddit_media` (
  `display_name` TEXT PRIMARY KEY,
  `banner_background_color` TEXT,
  `banner_background_image` TEXT,
  `banner_img` TEXT,
  `banner_size` TEXT,
  `community_icon` TEXT,
  `header_img` TEXT,
  `header_size` TEXT,
  `header_title` TEXT,
  `icon_img` TEXT,
  `icon_size` TEXT,
  `key_color` TEXT,
  `mobile_banner_image` TEXT,
  `primary_color` TEXT
);

CREATE TABLE `subreddit_permissions` (
  `display_name` TEXT PRIMARY KEY,
  `free_form_reports` bool,
  `link_flair_enabled` bool,
  `link_flair_position` TEXT,
  `user_can_flair_in_sr` bool,
  `user_flair_background_color` TEXT,
  `user_flair_css_class` TEXT,
  `user_flair_template_id` TEXT,
  `user_flair_text_color` TEXT
);

CREATE TABLE `subreddit_comment_media` (
  `display_name` TEXT PRIMARY KEY,
  `allowed_media_in_comments` TEXT
);

CREATE TABLE `subreddit_rules` (
  `rule_id` TEXT PRIMARY KEY,
  `subreddit` TEXT,
  `created_utc` integer,
  `description` TEXT,
  `kind` TEXT,
  `priority` integer,
  `short_name` TEXT,
  `violation_reason` TEXT
);

CREATE TABLE `banned` (
  `author_fullname` TEXT,
  `subreddit_id` TEXT,
  `banned_at_utc` integer,
  `banned_by` TEXT,
  PRIMARY KEY (`author_fullname`, `subreddit_id`)
);

CREATE TABLE `comment` (
  `id` TEXT PRIMARY KEY,
  `parent_id` TEXT,
  `body` TEXT,
  `author_fullname` TEXT,
  `created_utc` integer,
  `archived` bool,
  `downs` integer,
  `ups` integer,
  `edited` bool,
  `hidden` bool,
  `locked` bool,
  `permalink` TEXT,
  `removal_reason` TEXT,
  `removed_by` TEXT,
  `score` integer,
  `send_replies` bool,
  `spoiler` bool,
  `subreddit` TEXT,
  `thumbnail` TEXT,
  `thumbnail_height` integer,
  `thumbnail_width` integer,
  `title` TEXT,
  `total_awards_received` integer,
  `top_awarded_type` TEXT,
  `pinned` bool,
  `contest_mode` bool
);

CREATE TABLE `wiki` (
  `path` TEXT PRIMARY KEY,
  `subreddit` TEXT,
  `content` TEXT,
  `retrieved_on` integer,
  `revision_date` date
);

CREATE TABLE `revision_wiki` (
  `path` TEXT PRIMARY KEY,
  `revision_author` TEXT,
  `revision_author_id` TEXT,
  `revision_reason` TEXT
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