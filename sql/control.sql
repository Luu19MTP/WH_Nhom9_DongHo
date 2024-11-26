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

 Date: 26/11/2024 21:51:02
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
  `aggregate_file_path` varchar(500) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `load_mart_script_path` varchar(500) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  PRIMARY KEY (`mart_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb3 COLLATE = utf8mb3_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of config_mart
-- ----------------------------
INSERT INTO `config_mart` VALUES (1, 'Admin', 'localhost', 'D://DW/presentation/dongho_aggregate.csv', 'D://DW/presentation/load_to_mart.jar');

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
) ENGINE = InnoDB CHARACTER SET = utf8mb3 COLLATE = utf8mb3_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of file_log
-- ----------------------------

SET FOREIGN_KEY_CHECKS = 1;
