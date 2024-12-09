/*
 Navicat Premium Dump SQL

 Source Server         : default
 Source Server Type    : MySQL
 Source Server Version : 80035 (8.0.35)
 Source Host           : localhost:3306
 Source Schema         : control

 Target Server Type    : MySQL
 Target Server Version : 80035 (8.0.35)
 File Encoding         : 65001

 Date: 09/12/2024 21:50:12
*/
CREATE DATABASE IF NOT EXISTS control
CHARACTER SET utf8 COLLATE utf8_unicode_ci;

USE control;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for config_mart
-- ----------------------------
DROP TABLE IF EXISTS `config_mart`;
CREATE TABLE `config_mart`  (
  `mart_id` int NOT NULL,
  `username` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `remote_host` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `password` varchar(200) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `aggregate_file_path` varchar(500) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `load_mart_command` varchar(2000) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  PRIMARY KEY (`mart_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb3 COLLATE = utf8mb3_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of config_mart
-- ----------------------------
INSERT INTO `config_mart` VALUES (1, 'Admin', 'localhost', '0607', 'D://DW/presentation/dongho_aggregate.csv', 'java -jar \"D:\\DW\\presentation\\load_mart.jar\"');

-- ----------------------------
-- Table structure for config_source
-- ----------------------------
DROP TABLE IF EXISTS `config_source`;
CREATE TABLE `config_source`  (
  `source_id` int NOT NULL,
  `source_name` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `source_url` varchar(1000) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `source_file_location` varchar(500) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `file_format` varchar(200) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `scraping_script_path` varchar(500) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `destination_staging` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `transform_procedure` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `load_warehouse_procedure` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `aggregate_table` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `aggregate_procedure` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `dump_aggregate_file_path` varchar(500) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `mart_id` int NULL DEFAULT NULL,
  PRIMARY KEY (`source_id`) USING BTREE,
  INDEX `mart_id`(`mart_id` ASC) USING BTREE,
  CONSTRAINT `config_source_ibfk_1` FOREIGN KEY (`mart_id`) REFERENCES `config_mart` (`mart_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB CHARACTER SET = utf8mb3 COLLATE = utf8mb3_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of config_source
-- ----------------------------
INSERT INTO `config_source` VALUES (1, 'PNJ', 'https://www.pnj.com.vn/dong-ho/', 'D://DW/staging/data', 'pnj', 'D://DW/staging/extract/pnj_scraping.jar', 'dongho_pnj_daily_temp', 'proc_transform_pnj', 'proc_load_wh_pnj', 'dongho_aggregate', 'proc_aggregate', 'D://DW/staging/load/dongho_aggregate.csv', 1);
INSERT INTO `config_source` VALUES (2, 'Đăng Quang Watch', 'https://www.dangquangwatch.vn/dong-ho.html', 'D://DW/staging/data', 'dqw', 'D://DW/staging/extract/dqw_scraping.jar', 'dongho_dqw_daily_temp', 'proc_transform_dqw', 'proc_load_wh_dqw', 'dongho_aggregate', 'proc_aggregate', 'D://DW/staging/load/dongho_aggregate.csv', 1);

-- ----------------------------
-- Table structure for file_log
-- ----------------------------
DROP TABLE IF EXISTS `file_log`;
CREATE TABLE `file_log`  (
  `file_id` int NOT NULL AUTO_INCREMENT,
  `source_id` int NULL DEFAULT NULL,
  `file_path` varchar(1000) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `TIME` datetime NULL DEFAULT NULL,
  `count` int NULL DEFAULT NULL,
  `size` double NULL DEFAULT NULL,
  `STATUS` varchar(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `execute_time` datetime NULL DEFAULT NULL,
  PRIMARY KEY (`file_id`) USING BTREE,
  INDEX `source_id`(`source_id` ASC) USING BTREE,
  CONSTRAINT `file_log_ibfk_1` FOREIGN KEY (`source_id`) REFERENCES `config_source` (`source_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 5 CHARACTER SET = utf8mb3 COLLATE = utf8mb3_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of file_log
-- ----------------------------
INSERT INTO `file_log` VALUES (1, 1, 'D://DW/staging/data/pnj_26112024.csv', '2024-11-25 21:55:03', 11, 1981, 'MR', '2024-11-26 21:55:03');
INSERT INTO `file_log` VALUES (2, 1, 'D://DW/staging/data/pnj_6122024.csv', '2024-12-06 15:02:25', 11, 2992, 'SC', '2024-12-06 21:25:38');
INSERT INTO `file_log` VALUES (3, 1, 'D://DW/staging/data/pnj_09122024.csv', '2024-12-09 20:09:26', 21, 6042, 'SC', '2024-12-09 20:19:48');
INSERT INTO `file_log` VALUES (4, 2, 'D://DW/staging/data/dqw_09122024.csv', '2024-12-09 21:40:40', 21, 5108, 'SC', '2024-12-09 21:43:12');

-- ----------------------------
-- Table structure for process_log
-- ----------------------------
DROP TABLE IF EXISTS `process_log`;
CREATE TABLE `process_log`  (
  `process_id` int NOT NULL AUTO_INCREMENT,
  `source_id` int NULL DEFAULT NULL,
  `process_code` varchar(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `process_name` varchar(2000) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `started_at` datetime NULL DEFAULT NULL,
  `STATUS` varchar(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `updated_at` datetime NULL DEFAULT NULL,
  PRIMARY KEY (`process_id`) USING BTREE,
  INDEX `source_id`(`source_id` ASC) USING BTREE,
  CONSTRAINT `process_log_ibfk_1` FOREIGN KEY (`source_id`) REFERENCES `config_source` (`source_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 35 CHARACTER SET = utf8mb3 COLLATE = utf8mb3_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of process_log
-- ----------------------------
INSERT INTO `process_log` VALUES (1, 1, 'P1', 'Lấy dữ liệu từ nguồn về file', '2024-12-06 14:58:29', 'FL', '2024-12-06 14:58:30');
INSERT INTO `process_log` VALUES (2, 1, 'P1', 'Lấy dữ liệu từ nguồn về file', '2024-12-06 15:01:35', 'SC', '2024-12-06 15:02:25');
INSERT INTO `process_log` VALUES (3, 1, 'P1', 'Lấy dữ liệu từ nguồn về file', '2024-12-06 15:08:24', 'FL', '2024-12-06 15:08:24');
INSERT INTO `process_log` VALUES (4, 1, 'P2', 'Load dữ liệu từ file sang db.staging', '2024-12-06 16:28:04', 'SC', '2024-12-06 16:28:04');
INSERT INTO `process_log` VALUES (5, 1, 'P2', 'Load dữ liệu từ file sang db.staging', '2024-12-06 16:32:13', 'FL', '2024-12-06 16:32:14');
INSERT INTO `process_log` VALUES (6, 1, 'P2', 'Load dữ liệu từ file sang db.staging', '2024-12-06 16:33:06', 'FL', '2024-12-06 16:33:06');
INSERT INTO `process_log` VALUES (7, 1, 'P2', 'Load dữ liệu từ file sang db.staging', '2024-12-06 16:33:47', 'SC', '2024-12-06 16:33:47');
INSERT INTO `process_log` VALUES (8, 1, 'P3', 'Transform dữ liệu trong db.staging', '2024-12-06 19:43:53', 'FL', '2024-12-06 19:43:53');
INSERT INTO `process_log` VALUES (9, 1, 'P3', 'Transform dữ liệu trong db.staging', '2024-12-06 19:44:46', 'SC', '2024-12-06 19:44:46');
INSERT INTO `process_log` VALUES (10, 1, 'P3', 'Transform dữ liệu trong db.staging', '2024-12-06 19:49:42', 'FL', '2024-12-06 19:49:42');
INSERT INTO `process_log` VALUES (11, 1, 'P4', 'Load dữ liệu từ db.staging vào db.warehouse', '2024-12-06 21:24:29', 'FL', '2024-12-06 21:24:30');
INSERT INTO `process_log` VALUES (12, 1, 'P4', 'Load dữ liệu từ db.staging vào db.warehouse', '2024-12-06 21:25:25', 'FL', '2024-12-06 21:25:26');
INSERT INTO `process_log` VALUES (13, 1, 'P4', 'Load dữ liệu từ db.staging vào db.warehouse', '2024-12-06 21:25:38', 'SC', '2024-12-06 21:25:38');
INSERT INTO `process_log` VALUES (14, 1, 'P5', 'Tạo aggregate table trong db.warehouse', '2024-12-08 11:24:49', 'SC', '2024-12-08 11:24:50');
INSERT INTO `process_log` VALUES (15, 1, 'P6', 'Load dữ liệu từ aggregate table vào data mart', '2024-12-08 14:51:29', 'FL', '2024-12-08 14:51:30');
INSERT INTO `process_log` VALUES (16, 1, 'P6', 'Load dữ liệu từ aggregate table vào data mart', '2024-12-08 14:52:40', 'FL', '2024-12-08 14:52:40');
INSERT INTO `process_log` VALUES (17, 1, 'P6', 'Load dữ liệu từ aggregate table vào data mart', '2024-12-08 14:53:12', 'FL', '2024-12-08 14:53:14');
INSERT INTO `process_log` VALUES (18, 1, 'P6', 'Load dữ liệu từ aggregate table vào data mart', '2024-12-08 14:54:56', 'FL', '2024-12-08 14:54:57');
INSERT INTO `process_log` VALUES (19, 1, 'P6', 'Load dữ liệu từ aggregate table vào data mart', '2024-12-08 14:56:10', 'SC', '2024-12-08 14:56:11');
INSERT INTO `process_log` VALUES (20, 1, 'P6', 'Load dữ liệu từ aggregate table vào data mart', '2024-12-08 14:58:02', 'SC', '2024-12-08 14:58:03');
INSERT INTO `process_log` VALUES (21, 1, 'P1', 'Lấy dữ liệu từ nguồn về file', '2024-12-09 20:06:58', 'SC', '2024-12-09 20:09:26');
INSERT INTO `process_log` VALUES (22, 1, 'P2', 'Load dữ liệu từ file sang db.staging', '2024-12-09 20:12:05', 'SC', '2024-12-09 20:12:05');
INSERT INTO `process_log` VALUES (23, 1, 'P2', 'Load dữ liệu từ file sang db.staging', '2024-12-09 20:14:58', 'SC', '2024-12-09 20:14:58');
INSERT INTO `process_log` VALUES (24, 1, 'P3', 'Transform dữ liệu trong db.staging', '2024-12-09 20:15:41', 'FL', '2024-12-09 20:15:42');
INSERT INTO `process_log` VALUES (25, 1, 'P3', 'Transform dữ liệu trong db.staging', '2024-12-09 20:17:26', 'FL', '2024-12-09 20:17:26');
INSERT INTO `process_log` VALUES (26, 1, 'P3', 'Transform dữ liệu trong db.staging', '2024-12-09 20:17:51', 'SC', '2024-12-09 20:17:52');
INSERT INTO `process_log` VALUES (27, 1, 'P4', 'Load dữ liệu từ db.staging vào db.warehouse', '2024-12-09 20:19:47', 'SC', '2024-12-09 20:19:48');
INSERT INTO `process_log` VALUES (28, 2, 'P1', 'Lấy dữ liệu từ nguồn về file', '2024-12-09 21:40:04', 'SC', '2024-12-09 21:40:40');
INSERT INTO `process_log` VALUES (29, 2, 'P2', 'Load dữ liệu từ file sang db.staging', '2024-12-09 21:41:34', 'SC', '2024-12-09 21:41:35');
INSERT INTO `process_log` VALUES (30, 2, 'P3', 'Transform dữ liệu trong db.staging', '2024-12-09 21:42:14', 'SC', '2024-12-09 21:42:15');
INSERT INTO `process_log` VALUES (31, 2, 'P4', 'Load dữ liệu từ db.staging vào db.warehouse', '2024-12-09 21:43:12', 'SC', '2024-12-09 21:43:12');
INSERT INTO `process_log` VALUES (32, 2, 'P5', 'Tạo aggregate table trong db.warehouse', '2024-12-09 21:44:42', 'SC', '2024-12-09 21:44:42');
INSERT INTO `process_log` VALUES (33, 2, 'P6', 'Load dữ liệu từ aggregate table vào data mart', '2024-12-09 21:45:31', 'FL', '2024-12-09 21:45:33');
INSERT INTO `process_log` VALUES (34, 2, 'P6', 'Load dữ liệu từ aggregate table vào data mart', '2024-12-09 21:48:02', 'SC', '2024-12-09 21:48:03');

SET FOREIGN_KEY_CHECKS = 1;
