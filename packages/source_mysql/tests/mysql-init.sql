-- 创建测试表
-- =================================================================
-- Table structure for user (用户信息表)
-- =================================================================
CREATE TABLE `user` (
  `user_id` INT NOT NULL AUTO_INCREMENT COMMENT '用户主键ID',
  `email` VARCHAR(128) NULL,
  `openid_user_id` VARCHAR(50) NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =================================================================
-- Table structure for dataset (数据信息表)
-- =================================================================
CREATE TABLE `dataset` (
  `dataset_id` INT NOT NULL AUTO_INCREMENT COMMENT '数据集主键ID',
  `user_id` INT NULL COMMENT '外键，关联到用户表',
  `status` TINYINT(1) NULL,
  `create_time` DATETIME NULL,
  `title` VARCHAR(300) NULL,
  `description` MEDIUMTEXT NULL,
  `is_deleted` TINYINT(1) DEFAULT 0,
  `is_checked` INT NULL,
  `is_review` INT NULL,
  `backup_status` INT NULL,
  `path` VARCHAR(60) NULL,
  PRIMARY KEY (`dataset_id`),
  CONSTRAINT `fk_dataset_user` FOREIGN KEY (`user_id`) REFERENCES `user` (`user_id`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =================================================================
-- Table structure for file (文件信息表)
-- =================================================================
CREATE TABLE `file` (
  `file_id` INT NOT NULL AUTO_INCREMENT COMMENT '文件主键ID',
  `dataset_id` INT NULL COMMENT '外键，关联到数据信息表(dataset)',
  `file_name` VARCHAR(300) NULL,
  `is_deleted` TINYINT(1) DEFAULT 0,
  `relative_path` VARCHAR(500) NULL,
  `file_size` VARCHAR(100) NULL,
  `file_suffix` VARCHAR(300) NULL,
  `file_code` VARCHAR(300) NULL,
  `md5` VARCHAR(300) NULL,
  `status` VARCHAR(30) NULL,
  `create_time` DATETIME NULL,
  PRIMARY KEY (`file_id`),
  CONSTRAINT `fk_file_dataset` FOREIGN KEY (`dataset_id`) REFERENCES `dataset` (`dataset_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 启用 binlog需要依赖容器启动参数配置
SET GLOBAL binlog_format = ROW;

-- 创建必要的权限（测试专用）
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'testuser'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON testdb.* TO 'testuser'@'%';
FLUSH PRIVILEGES;

-- =================================================================
-- 插入测试数据 (5条/表)
-- =================================================================

-- user表数据
INSERT INTO `user` (`email`, `openid_user_id`) VALUES
('user1@example.com', 'openid1'),
('user2@example.com', 'openid2'),
('user3@example.com', 'openid3'),
('user4@example.com', 'openid4'),
('user5@example.com', 'openid5');

-- dataset表数据
INSERT INTO `dataset` (
  `user_id`, `status`, `create_time`, `title`,
  `description`, `is_checked`, `is_review`, `backup_status`, `path`
) VALUES
(1, 1, NOW(), 'Dataset 1', 'Description 1', 1, 0, 0, '/datasets/1'),
(2, 1, NOW(), 'Dataset 2', 'Description 2', 1, 0, 0, '/datasets/2'),
(3, 1, NOW(), 'Dataset 3', 'Description 3', 1, 0, 0, '/datasets/3'),
(4, 1, NOW(), 'Dataset 4', 'Description 4', 1, 0, 0, '/datasets/4'),
(5, 1, NOW(), 'Dataset 5', 'Description 5', 1, 0, 0, '/datasets/5');

-- file表数据
INSERT INTO `file` (
  `dataset_id`, `file_name`, `relative_path`, `file_size`,
  `file_suffix`, `file_code`, `md5`, `status`, `create_time`
) VALUES
(1, 'file1.txt', 'files/1', '1024', 'txt', 'F001', 'md51', 'active', NOW()),
(2, 'file2.jpg', 'files/2', '2048', 'jpg', 'F002', 'md52', 'active', NOW()),
(3, 'file3.pdf', 'files/3', '3072', 'pdf', 'F003', 'md53', 'active', NOW()),
(4, 'file4.png', 'files/4', '4096', 'png', 'F004', 'md54', 'active', NOW()),
(5, 'file5.doc', 'files/5', '5120', 'doc', 'F005', 'md55', 'active', NOW());
