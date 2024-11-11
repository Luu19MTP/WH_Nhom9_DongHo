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

 Date: 12/11/2024 01:36:24
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for config_source
-- ----------------------------
DROP TABLE IF EXISTS `config_source`;
CREATE TABLE `config_source`  (
  `source_id` int NOT NULL AUTO_INCREMENT,
  `source_name` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `source_url` varchar(1000) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `source_file_location` varchar(500) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `file_format` varchar(200) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `scraping_script_path` varchar(500) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `destination_staging` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `destination_transform` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `transform_procedure` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `destination_warehouse` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `load_warehouse_procedure` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  PRIMARY KEY (`source_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8mb3 COLLATE = utf8mb3_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of config_source
-- ----------------------------
INSERT INTO `config_source` VALUES (1, 'PNJ', 'https://www.pnj.com.vn/dong-ho/', 'D://DW/staging/data', 'pnj', 'D://DW/staging/extract/pnj_scraping.jar', 'dongho_pnj_daily_temp', 'dongho_pnj_daily', 'proc_transform_pnj', 'dongho', '');

-- ----------------------------
-- Table structure for file_log
-- ----------------------------
DROP TABLE IF EXISTS `file_log`;
CREATE TABLE `file_log`  (
  `file_id` int NOT NULL AUTO_INCREMENT,
  `source_id` int NULL DEFAULT NULL,
  `file_path` varchar(200) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `time` datetime NULL DEFAULT NULL,
  `count` int NULL DEFAULT NULL,
  `size` double NULL DEFAULT NULL,
  `status` varchar(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NULL DEFAULT NULL,
  `execute_time` datetime NULL DEFAULT NULL,
  PRIMARY KEY (`file_id`) USING BTREE,
  INDEX `source_id`(`source_id` ASC) USING BTREE,
  CONSTRAINT `file_log_ibfk_1` FOREIGN KEY (`source_id`) REFERENCES `config_source` (`source_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 4 CHARACTER SET = utf8mb3 COLLATE = utf8mb3_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of file_log
-- ----------------------------
INSERT INTO `file_log` VALUES (1, 1, 'D://DW/staging/data/pnj_4112024.csv', '2024-11-04 21:49:00', 11, 4176, 'SC', '2024-11-04 21:50:00');
INSERT INTO `file_log` VALUES (2, 1, 'D://DW/staging/data/pnj_11112024.csv', '2024-11-11 23:50:00', 11, 4176, 'SC', '2024-11-11 00:56:00');
INSERT INTO `file_log` VALUES (3, 1, 'D://DW/staging/data/pnj_12112024.csv', '2024-11-12 00:01:00', 11, 4176, 'SC', '2024-11-12 01:24:00');

SET FOREIGN_KEY_CHECKS = 1;
