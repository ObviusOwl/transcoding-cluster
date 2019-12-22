CREATE TABLE `task_dependencies` (
  `task_id` int(11) NOT NULL,
  `dep_id` int(11) NOT NULL,
  UNIQUE KEY `task_id` (`task_id`,`dep_id`),
  CONSTRAINT `dep_id`  FOREIGN KEY (`dep_id`)  REFERENCES `tasks` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
  CONSTRAINT `task_id` FOREIGN KEY (`task_id`) REFERENCES `tasks` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=INNODB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE `tasks` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `command` text COLLATE utf8_unicode_ci DEFAULT NULL,
  `worker_id` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `status` int(11) NOT NULL DEFAULT 0,
  `priority` int(11) NOT NULL DEFAULT 0,
  `affinity` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE `workers` (
  `id` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `drain` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

