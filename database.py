"""
Модуль взаимодействия с базой данных MySQL-server.
Все действия с базой необходимо выполнять с помощью экземпляра Database данного модуля
"""

import logging
from collections import OrderedDict
from typing import List, Dict

import mysql.connector
from mysql.connector import errorcode
from mysql.connector.cursor import MySQLCursorNamedTuple

import constants
import suppliers


class Database:
    """Реализует связь с базой данных поставщиков"""

    TABLES = OrderedDict()

    TABLES['supplier'] = """
        CREATE TABLE IF NOT EXISTS `supplier`
          (
            `supplier_id` TINYINT(4) NOT NULL,
            `name` VARCHAR(256) NOT NULL,
            PRIMARY KEY (`supplier_id`),
            UNIQUE KEY `supplier_supplier_id_uindex` (`supplier_id`),
            UNIQUE KEY `supplier_name_uindex` (`name`)
          )
          ENGINE = InnoDB;
    """

    TABLES['supplier_brand'] = """
        CREATE TABLE IF NOT EXISTS `supplier_brand`
          (
            `supplier_brand_id` INT(11) NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(256) NOT NULL,
            `supplier_id` TINYINT(4) NOT NULL,
            PRIMARY KEY (`supplier_brand_id`),
            UNIQUE KEY `supplier_brand_uk` (`name`, `supplier_id`),
            KEY `supplier_brand_supplier_supplier_id_fk` (`supplier_id`),
            CONSTRAINT `supplier_brand_supplier_supplier_id_fk`
              FOREIGN KEY (`supplier_id`)
                REFERENCES `supplier` (`supplier_id`)
          )
          ENGINE = InnoDB;
    """

    TABLES['brand'] = """
        CREATE TABLE IF NOT EXISTS `brand`
          (
            `brand_id` INT(11) NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(256) NOT NULL,
            `active` TINYINT(1) NOT NULL DEFAULT '0',
            PRIMARY KEY (`brand_id`),
            UNIQUE KEY `brand_name_uindex` (`name`)
          )
          ENGINE = InnoDB;
    """

    TABLES['brand_supplier_brand'] = """
        CREATE TABLE IF NOT EXISTS `brand_supplier_brand`
          (
            `brand_id` INT(11) NOT NULL,
            `supplier_brand_id` INT(11) NOT NULL,
            KEY `brand_supplier_brand_brand_brand_id_fk` (`brand_id`),
            KEY `brand_supplier_brand_supplier_brand_supplier_brand_id_fk` (`supplier_brand_id`),
            CONSTRAINT `brand_supplier_brand_brand_brand_id_fk`
              FOREIGN KEY (`brand_id`)
                REFERENCES `brand` (`brand_id`)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            CONSTRAINT `brand_supplier_brand_supplier_brand_supplier_brand_id_fk`
              FOREIGN KEY (`supplier_brand_id`)
                REFERENCES `supplier_brand` (`supplier_brand_id`)
                ON DELETE CASCADE
                ON UPDATE CASCADE
          )
          ENGINE = InnoDB;
    """

    TABLES['supplier_item'] = """
        CREATE TABLE IF NOT EXISTS `supplier_item`
          (
            `supplier_item_id` INT(11) NOT NULL AUTO_INCREMENT,
            `supplier_brand_id` INT(11) NOT NULL,
            `norm_mpn` VARCHAR(256) NOT NULL,
            `available` TINYINT(1) NOT NULL DEFAULT '0',
            `prefix` VARCHAR(10) NOT NULL,
            `mpn` VARCHAR(256) NOT NULL,
            `number` VARCHAR(266) NOT NULL,
            PRIMARY KEY (`supplier_item_id`),
            UNIQUE KEY `supplier_item_uk` (`supplier_brand_id`, `norm_mpn`),
            KEY `supplier_item_number_index` (`number`),
            CONSTRAINT `supplier_item_supplier_brand_supplier_brand_id_fk`
              FOREIGN KEY (`supplier_brand_id`)
                REFERENCES `supplier_brand` (`supplier_brand_id`)
                ON DELETE CASCADE
                ON UPDATE CASCADE
          )
          ENGINE = InnoDB;
    """

    TABLES['keystone_item'] = """
        CREATE TABLE IF NOT EXISTS `keystone_item`
          (
            `supplier_item_id` INT(11) NOT NULL,
            `LongDescription` VARCHAR(5000) DEFAULT NULL,
            `JobberPrice` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `Cost` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `Fedexable` TINYINT(1) DEFAULT NULL,
            `ExeterQty` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `MidWestQty` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `SouthEastQty` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `TexasQty` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `PacificNWQty` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `GreatLakesQty` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `CaliforniaQty` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `TotalQty` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `UPCCode` VARCHAR(100) DEFAULT NULL,
            `Prop65Toxicity` ENUM ('B', 'C', 'N', 'R') DEFAULT NULL,
            `HazardousMaterial` TINYINT(1) DEFAULT NULL,
            PRIMARY KEY (`supplier_item_id`),
            CONSTRAINT `keystone_item_supplier_item_supplier_item_id_fk`
              FOREIGN KEY (`supplier_item_id`)
                REFERENCES `supplier_item` (`supplier_item_id`)
                ON DELETE CASCADE
                ON UPDATE CASCADE
          )
          ENGINE = InnoDB;
    """

    TABLES['meyer_category'] = """
        CREATE TABLE IF NOT EXISTS `meyer_category`
          (
            `meyer_category_id` INT(11) NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(256) NOT NULL,
            PRIMARY KEY (`meyer_category_id`),
            UNIQUE KEY `meyer_category_name_uindex` (`name`)
          )
          ENGINE = InnoDB;
    """

    TABLES['meyer_subcategory'] = """
        CREATE TABLE IF NOT EXISTS `meyer_subcategory`
          (
            `meyer_subcategory_id` INT(11) NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(256) NOT NULL,
            PRIMARY KEY (`meyer_subcategory_id`),
            UNIQUE KEY `meyer_subcategory_name_uindex` (`name`)
          )
          ENGINE = InnoDB;
    """

    TABLES['meyer_item'] = """
        CREATE TABLE IF NOT EXISTS `meyer_item`
          (
            `supplier_item_id` INT(11) NOT NULL,
            `Jobber_Price` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `Customer_Price` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `SuggestedRetailPrice` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `UPC` VARCHAR(100) DEFAULT NULL,
            `MAP` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `Weight` DECIMAL(8, 2) UNSIGNED DEFAULT NULL,
            `Height` DECIMAL(8, 2) UNSIGNED DEFAULT NULL,
            `Length` DECIMAL(8, 2) UNSIGNED DEFAULT NULL,
            `Width` DECIMAL(8, 2) UNSIGNED DEFAULT NULL,
            `Description` VARCHAR(5000) DEFAULT NULL,
            `Qty_008` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_032` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_041` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_044` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_053` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_062` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_063` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_065` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_068` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_069` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_070` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_071` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_072` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_077` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_093` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_094` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_098` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `QtyAvailable` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Discontinued` TINYINT(1) DEFAULT NULL,
            `Kit` TINYINT(1) DEFAULT NULL,
            `Kit_Only` TINYINT(1) DEFAULT NULL,
            `LTL_Required` TINYINT(1) DEFAULT NULL,
            `LTL_Eligible` TINYINT(1) DEFAULT NULL,
            `Oversize` TINYINT(1) DEFAULT NULL,
            `meyer_category_id` INT(11) DEFAULT NULL,
            `meyer_subcategory_id` INT(11) DEFAULT NULL,
            PRIMARY KEY (`supplier_item_id`),
            KEY `meyer_item_meyer_category_meyer_category_id_fk` (`meyer_category_id`),
            KEY `meyer_item_meyer_subcategory_meyer_subcategory_id_fk` (`meyer_subcategory_id`),
            CONSTRAINT `meyer_item_meyer_category_meyer_category_id_fk`
              FOREIGN KEY (`meyer_category_id`)
                REFERENCES `meyer_category` (`meyer_category_id`)
                ON DELETE SET NULL
                ON UPDATE CASCADE,
            CONSTRAINT `meyer_item_meyer_subcategory_meyer_subcategory_id_fk`
              FOREIGN KEY (`meyer_subcategory_id`)
                REFERENCES `meyer_subcategory` (`meyer_subcategory_id`)
                ON DELETE SET NULL
                ON UPDATE CASCADE,
            CONSTRAINT `meyer_item_supplier_item_supplier_item_id_fk`
              FOREIGN KEY (`supplier_item_id`)
                REFERENCES `supplier_item` (`supplier_item_id`)
                ON DELETE CASCADE
                ON UPDATE CASCADE
          )
          ENGINE = InnoDB;
    """

    TABLES['premier_item'] = """
        CREATE TABLE IF NOT EXISTS `premier_item`
          (
            `supplier_item_id` INT(11) NOT NULL,
            `Distributor_Cost` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `Package_Quantity` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Core_Price` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `UPC` VARCHAR(100) DEFAULT NULL,
            `Part_Description` VARCHAR(5000) DEFAULT NULL,
            `Inventory_Count` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Inventory_Type` ENUM ('Discontinued', 'NonStocking', 'Stocking') DEFAULT NULL,
            `cost_usd` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `jobber_usd` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `map_usd` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `retail_usd` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `cost_cad` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `jobber_cad` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `map_cad` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `retail_cad` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `Qty_UT_1_US` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_KY_1_US` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_TX_1_US` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_CA_1_US` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_AB_1_CA` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_WA_1_US` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_CO_1_US` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Qty_PO_1_CA` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            PRIMARY KEY (`supplier_item_id`),
            CONSTRAINT `premier_item_supplier_item_supplier_item_id_fk`
              FOREIGN KEY (`supplier_item_id`)
                REFERENCES `supplier_item` (`supplier_item_id`)
                ON DELETE CASCADE
                ON UPDATE CASCADE
          )
          ENGINE = InnoDB;
    """

    TABLES['trans_item'] = """
        CREATE TABLE IF NOT EXISTS `trans_item`
          (
            `supplier_item_id` INT(11) NOT NULL,
            `CA` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `TX` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `FL` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `CO` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `OH` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `ID` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `PA` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `LIST_PRICE` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `JOBBER_PRICE` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `TOTAL` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `STATUS` ENUM ('A', 'B', 'D', 'K', 'M', 'N', 'R') DEFAULT NULL,
            `DESCRIPTION` VARCHAR(5000) DEFAULT NULL,
            `YOUR_PRICE` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `MAP_CONFIRM_W_JOBBER` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `CORE_PRICE` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `FEDERAL_EXCISE_TAX` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `OVERSIZE` TINYINT(1) DEFAULT NULL,            
            PRIMARY KEY (`supplier_item_id`),
            CONSTRAINT `trans_item_supplier_item_supplier_item_id_fk`
              FOREIGN KEY (`supplier_item_id`)
                REFERENCES `supplier_item` (`supplier_item_id`)
                ON DELETE CASCADE
                ON UPDATE CASCADE
          )
          ENGINE = InnoDB;
    """

    TABLES['turn14_category'] = """
        CREATE TABLE IF NOT EXISTS `turn14_category`
          (
            `turn14_category_id` INT(11) NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(256) NOT NULL,
            PRIMARY KEY (`turn14_category_id`),
            UNIQUE KEY `turn14_category_name_uindex` (`name`)
          )
          ENGINE = InnoDB;
    """

    TABLES['turn14_product_name'] = """
        CREATE TABLE IF NOT EXISTS `turn14_product_name`
          (
            `turn14_product_name_id` INT(11) NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(256) NOT NULL,
            PRIMARY KEY (`turn14_product_name_id`),
            UNIQUE KEY `turn14_product_name_value_uindex` (`name`)
          )
          ENGINE = InnoDB;
    """

    TABLES['turn14_subcategory'] = """
        CREATE TABLE IF NOT EXISTS `turn14_subcategory`
          (
            `turn14_subcategory_id` INT(11) NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(256) NOT NULL,
            PRIMARY KEY (`turn14_subcategory_id`),
            UNIQUE KEY `turn14_subcategory_name_uindex` (`name`)
          )
          ENGINE = InnoDB;
    """

    TABLES['turn14_item'] = """
        CREATE TABLE IF NOT EXISTS `turn14_item`
          (
            `supplier_item_id` INT(11) NOT NULL,
            `Description` VARCHAR(5000) DEFAULT NULL,
            `Cost` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `Retail` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `Jobber` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `CoreCharge` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `Map` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `Other` DECIMAL(10, 4) UNSIGNED DEFAULT NULL,
            `OtherName` ENUM ('Dealer', 'Net Dealer', 'WD') DEFAULT NULL,
            `EastStock` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `WestStock` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `CentralStock` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `Stock` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `MfrStock` MEDIUMINT(8) UNSIGNED DEFAULT NULL,
            `MfrStockDate` DATE DEFAULT NULL,
            `DropShip` ENUM ('always', 'never', 'possible') DEFAULT NULL,
            `DSFee` VARCHAR(100) DEFAULT NULL,
            `Weight` DECIMAL(8, 2) UNSIGNED DEFAULT NULL,
            `item_id_in_api` VARCHAR(20) DEFAULT NULL,
            `turn14_category_id` INT(11) DEFAULT NULL,
            `turn14_subcategory_id` INT(11) DEFAULT NULL,
            `turn14_product_name_id` INT(11) DEFAULT NULL,
            `LxWxH-W` VARCHAR(256) DEFAULT NULL,
            `barcode` VARCHAR(50) DEFAULT NULL,
            PRIMARY KEY (`supplier_item_id`),
            UNIQUE KEY `turn14_item_item_id_in_api_uindex` (`item_id_in_api`),
            KEY `turn14_item_turn14_category_turn14_category_id_fk` (`turn14_category_id`),
            KEY `turn14_item_turn14_subcategory_turn14_subcategory_id_fk` (`turn14_subcategory_id`),
            KEY `turn14_item_turn14_product_name_turn14_product_name_id_fk` (`turn14_product_name_id`),
            CONSTRAINT `turn14_item_supplier_item_supplier_item_id_fk`
              FOREIGN KEY (`supplier_item_id`)
                REFERENCES `supplier_item` (`supplier_item_id`)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            CONSTRAINT `turn14_item_turn14_category_turn14_category_id_fk`
              FOREIGN KEY (`turn14_category_id`)
                REFERENCES `turn14_category` (`turn14_category_id`)
                ON DELETE SET NULL
                ON UPDATE CASCADE,
            CONSTRAINT `turn14_item_turn14_product_name_turn14_product_name_id_fk`
              FOREIGN KEY (`turn14_product_name_id`)
                REFERENCES `turn14_product_name` (`turn14_product_name_id`)
                ON DELETE SET NULL
                ON UPDATE CASCADE,
            CONSTRAINT `turn14_item_turn14_subcategory_turn14_subcategory_id_fk`
              FOREIGN KEY (`turn14_subcategory_id`)
                REFERENCES `turn14_subcategory` (`turn14_subcategory_id`)
                ON DELETE SET NULL
                ON UPDATE CASCADE
          )
          ENGINE = InnoDB;     
    """

    TABLES['turn14_fitment'] = """
        CREATE TABLE IF NOT EXISTS `turn14_fitment`
          (
            `turn14_item_id` INT(11) NOT NULL,
            `vehicle_id` INT(11) NOT NULL,
            PRIMARY KEY (`turn14_item_id`, `vehicle_id`),
            CONSTRAINT `turn14_fitment_turn14_item_supplier_item_id_fk`
              FOREIGN KEY (`turn14_item_id`)
                REFERENCES `turn14_item` (`supplier_item_id`)
                ON DELETE CASCADE
                ON UPDATE CASCADE
          )
          ENGINE = InnoDB;
    """

    TABLES['turn14_media_content'] = """
        CREATE TABLE IF NOT EXISTS `turn14_media_content`
          (
            `turn14_media_content_id` INT(11) NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(256) NOT NULL,
            PRIMARY KEY (`turn14_media_content_id`),
            UNIQUE KEY `turn14_media_content_name_uindex` (`name`)
          )
          ENGINE = InnoDB;
    """

    TABLES['turn14_url'] = """
        CREATE TABLE IF NOT EXISTS `turn14_url`
          (
            `turn14_url_id` INT(11) NOT NULL AUTO_INCREMENT,
            `value` VARCHAR(256) NOT NULL,
            `height` SMALLINT(6) UNSIGNED DEFAULT NULL,
            `width` SMALLINT(6) UNSIGNED DEFAULT NULL,
            PRIMARY KEY (`turn14_url_id`),
            UNIQUE KEY `turn14_url_value_uindex` (`value`)
          )
          ENGINE = InnoDB;
    """

    TABLES['turn14_files'] = """
        CREATE TABLE IF NOT EXISTS `turn14_files`
          (
            `turn14_item_id` INT(11) NOT NULL,
            `turn14_url_id` INT(11) NOT NULL,
            `turn14_media_content_id` INT(11) DEFAULT NULL,
            PRIMARY KEY (`turn14_item_id`, `turn14_url_id`),
            KEY `turn14_files_turn14_url_turn14_url_id_fk` (`turn14_url_id`),
            KEY `turn14_files_turn14_media_content_turn14_media_content_id_fk` (`turn14_media_content_id`),
            CONSTRAINT `turn14_files_turn14_item_supplier_item_id_fk`
              FOREIGN KEY (`turn14_item_id`)
                REFERENCES `turn14_item` (`supplier_item_id`)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            CONSTRAINT `turn14_files_turn14_media_content_turn14_media_content_id_fk`
              FOREIGN KEY (`turn14_media_content_id`)
                REFERENCES `turn14_media_content` (`turn14_media_content_id`)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            CONSTRAINT `turn14_files_turn14_url_turn14_url_id_fk`
              FOREIGN KEY (`turn14_url_id`)
                REFERENCES `turn14_url` (`turn14_url_id`)
                ON DELETE CASCADE
                ON UPDATE CASCADE
          )
          ENGINE = InnoDB;
    """

    PROCEDURES = OrderedDict()

    PROCEDURES['write_turn14_item_info_from_get_items_api'] = [
        """
            DROP PROCEDURE IF EXISTS write_turn14_item_info_from_get_items_api;
        """,
        """
            CREATE PROCEDURE write_turn14_item_info_from_get_items_api(IN p_supplier_item_id INT(11),
                                                                       IN p_item_id_in_api VARCHAR(20),
                                                                       IN p_product_name VARCHAR(256),
                                                                       IN p_category_name VARCHAR(256),
                                                                       IN p_subcategory_name VARCHAR(256),
                                                                       IN p_dimensions VARCHAR(256),
                                                                       IN p_thumbnail VARCHAR(256),
                                                                       IN p_barcode VARCHAR(50))
            BEGIN
              -- vars
              DECLARE my_turn14_product_name_id INT(11);
              DECLARE my_turn14_category_id INT(11);
              DECLARE my_turn14_subcategory_id INT(11);
              DECLARE my_turn14_url_id INT(11);
              DECLARE my_turn14_media_content_id INT(11);
            
              -- product_name:
              IF p_product_name IS NOT NULL THEN
                SELECT
                  turn14_product_name_id INTO my_turn14_product_name_id
                FROM turn14_product_name
                WHERE name = p_product_name;
            
                IF my_turn14_product_name_id IS NULL THEN
                  INSERT INTO turn14_product_name(name)
                  VALUES
                    (p_product_name);
                  SELECT
                    turn14_product_name_id INTO my_turn14_product_name_id
                  FROM turn14_product_name
                  WHERE name = p_product_name;
                END IF;
              END IF;
            
              -- category:
              IF p_category_name IS NOT NULL THEN
                SELECT
                  turn14_category_id INTO my_turn14_category_id
                FROM turn14_category
                WHERE name = p_category_name;
            
                IF my_turn14_category_id IS NULL THEN
                  INSERT INTO turn14_category(name)
                  VALUES
                    (p_category_name);
                  SELECT
                    turn14_category_id INTO my_turn14_category_id
                  FROM turn14_category
                  WHERE name = p_category_name;
                END IF;
              END IF;
            
              -- subcategory:
              IF p_subcategory_name IS NOT NULL THEN
                SELECT
                  turn14_subcategory_id INTO my_turn14_subcategory_id
                FROM turn14_subcategory
                WHERE name = p_subcategory_name;
            
                IF my_turn14_subcategory_id IS NULL THEN
                  INSERT INTO turn14_subcategory(name)
                  VALUES
                    (p_subcategory_name);
                  SELECT
                    turn14_subcategory_id INTO my_turn14_subcategory_id
                  FROM turn14_subcategory
                  WHERE name = p_subcategory_name;
                END IF;
              END IF;
            
              IF p_thumbnail IS NOT NULL THEN
                -- url:
                SELECT
                  turn14_url_id INTO my_turn14_url_id
                FROM turn14_url
                WHERE value = p_thumbnail;
            
                IF my_turn14_url_id IS NULL THEN
                  INSERT INTO turn14_url(value)
                  VALUES
                    (p_thumbnail);
                  SELECT
                    turn14_url_id INTO my_turn14_url_id
                  FROM turn14_url
                  WHERE value = p_thumbnail;
                END IF;
            
                -- media_content:
                SELECT
                  turn14_media_content_id INTO my_turn14_media_content_id
                FROM turn14_media_content
                WHERE name = 'thumbnail_turn14';
            
                IF my_turn14_media_content_id IS NULL THEN
                  INSERT INTO turn14_media_content(name)
                  VALUES
                    ('thumbnail_turn14');
                  SELECT
                    turn14_media_content_id INTO my_turn14_media_content_id
                  FROM turn14_media_content
                  WHERE name = 'thumbnail_turn14';
                END IF;
            
                -- file:
                INSERT IGNORE INTO turn14_files(turn14_item_id, turn14_url_id, turn14_media_content_id)
                VALUES
                  (p_supplier_item_id, my_turn14_url_id, my_turn14_media_content_id);
            
              END IF;
            
              -- turn14_item:
              UPDATE turn14_item
              SET item_id_in_api         = p_item_id_in_api,
                  turn14_category_id     = my_turn14_category_id,
                  turn14_subcategory_id  = my_turn14_subcategory_id,
                  turn14_product_name_id = my_turn14_product_name_id,
                  `LxWxH-W`              = p_dimensions,
                  barcode                = p_barcode
              WHERE supplier_item_id = p_supplier_item_id;
            END;
        """
    ]

    PROCEDURES['write_turn14_item_info_from_get_items_data_api'] = [
        """
            DROP PROCEDURE IF EXISTS write_turn14_item_info_from_get_items_data_api;
        """,
        """
            CREATE PROCEDURE write_turn14_item_info_from_get_items_data_api(IN p_item_id_in_api VARCHAR(20),
                                                                            IN p_url VARCHAR(256),
                                                                            IN p_media_content VARCHAR(256),
                                                                            IN p_height SMALLINT(6) UNSIGNED,
                                                                            IN p_width SMALLINT(6) UNSIGNED)
            BEGIN
              -- variables:
              DECLARE my_turn14_item_id INT(11);
              DECLARE my_turn14_url_id INT(11);
              DECLARE my_turn14_media_content_id INT(11);
            
              SELECT
                supplier_item_id INTO my_turn14_item_id
              FROM turn14_item
              WHERE item_id_in_api = p_item_id_in_api;
            
              IF my_turn14_item_id IS NOT NULL THEN
                -- url:
                SELECT
                  turn14_url_id INTO my_turn14_url_id
                FROM turn14_url
                WHERE value = p_url;
            
                IF my_turn14_url_id IS NULL THEN
                  INSERT INTO turn14_url(value, height, width)
                  VALUES
                    (p_url, p_height, p_width);
                  SELECT
                    turn14_url_id INTO my_turn14_url_id
                  FROM turn14_url
                  WHERE value = p_url;
                END IF;
            
                -- media_content:
                SELECT
                  turn14_media_content_id INTO my_turn14_media_content_id
                FROM turn14_media_content
                WHERE name = p_media_content;
            
                IF my_turn14_media_content_id IS NULL THEN
                  INSERT INTO turn14_media_content(name)
                  VALUES
                    (p_media_content);
                  SELECT
                    turn14_media_content_id INTO my_turn14_media_content_id
                  FROM turn14_media_content
                  WHERE name = p_media_content;
                END IF;
            
                -- file:
                INSERT IGNORE INTO turn14_files(turn14_item_id, turn14_url_id, turn14_media_content_id)
                VALUES
                  (my_turn14_item_id, my_turn14_url_id, my_turn14_media_content_id);
              END IF;
            END;
        """
    ]

    def __init__(
            self,
            *,
            create: bool = False,
            option_files=None,
            option_groups=None,
            **kwargs
    ):
        kwargs.update({'use_pure': True})

        if option_files:
            kwargs.update({'option_files': option_files})
            if option_groups:
                kwargs.update({'option_groups': option_groups})
            kwargs = mysql.connector.optionfiles.read_option_files(**kwargs)
        logging.debug('Параметры подключения: {}'.format(kwargs))

        try:
            db_name = None
            if create:
                db_name = kwargs.pop('database')

            logging.info('Установка соединения с сервером MySQL')
            self.connection = mysql.connector.connect(**kwargs)
            logging.info('Соединение установлено: OK')

            if create:
                self._connect_to_db(db_name)
                self._create_tables_if_needed()

            self._create_stored_procedures()
        except mysql.connector.Error as err:
            logging.error(str(err))
            raise

    def _connect_to_db(self, db_name: str):
        """
        Подключается к базе данных db_name
        """
        try:
            logging.debug('Попытка подключится к базе данных {}.'.format(db_name))
            self.connection.database = db_name
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.warning('База данных {} не существует.'.format(db_name))
                logging.debug('Попытка создать базу данных {}.'.format(db_name))
                self._create_db(db_name)
                self.connection.database = db_name
                logging.info('База данных {} успешно создана.'.format(db_name))
                logging.info('Подключение к базе данных {}: ОК.'.format(db_name))
            else:
                raise
        else:
            logging.info('Подключение к базе данных {}: ОК.'.format(db_name))

    def _create_db(self, db_name: str):
        statement = \
            """CREATE DATABASE IF NOT EXISTS {} CHARACTER SET = utf8mb4 COLLATE utf8mb4_general_ci;""".format(db_name)
        self.execute_without_results(statement, many=False, commit=False)

    def _create_tables_if_needed(self):
        logging.info('{} Создание таблиц базы данных в случае их отсутствия.'.format(constants.LOGGING_START))
        for table in self.TABLES:
            logging.debug('Создание таблицы {}.'.format(table))
            statement = self.TABLES[table]
            self.execute_without_results(statement, many=False, commit=False)
            logging.debug('Таблица {} создана: OK.'.format(table))

        self.execute_without_results(
            statement="""INSERT IGNORE INTO supplier(supplier_id, name) VALUES (%s, %s);""",
            data=((1, 'Keystone'), (2, 'Meyer'), (3, 'Premier'), (4, 'Trans'), (5, 'Turn14')),
            many=True,
            commit=True
        )
        logging.info('{} Создание таблиц базы данных в случае их отсутствия.'.format(constants.LOGGING_FINISH))

    def _create_stored_procedures(self):
        """
        Создание или обновление хранимых процедур базы данных
        """
        logging.info('{} Создание или обновление хранимых процедур базы данных.'.format(constants.LOGGING_START))
        for procedure in self.PROCEDURES:
            logging.debug('Создание процедуры {}'.format(procedure))
            for statement in self.PROCEDURES[procedure]:
                self.execute_without_results(statement, many=False, commit=False)
            logging.debug('Процедура {} создана: OK.'.format(procedure))
        logging.info('{} Создание или обновление хранимых процедур базы данных.'.format(constants.LOGGING_FINISH))

    def __del__(self):
        """Закрывает соединение с сервером MySQL"""
        if hasattr(self, 'connection') and self.connection.is_connected():
            self.connection.close()
            logging.debug('Закрыто соединение с сервером MySQL')

    def insert_into_supplier_brand(self, data: List[Dict]) -> None:
        statement = """
            INSERT INTO supplier_brand(name, supplier_id)
            VALUES
                (%(name)s, %(supplier_id)s)
            ON DUPLICATE KEY UPDATE 
                name=values(name),
                supplier_id = values(supplier_id);    
        """
        self.execute_without_results(statement, data, many=True, commit=True)

    def insert_into_supplier_item(self, data: List[Dict]) -> None:
        statement = """
            INSERT INTO supplier_item(supplier_brand_id, norm_mpn, available, prefix, mpn, number)
              VALUES
                (
                  %(supplier_brand_id)s,
                  %(norm_mpn)s,
                  %(available)s,
                  %(prefix)s,
                  %(mpn)s,
                  %(number)s
                )
              ON DUPLICATE KEY UPDATE
                available = values(available),
                prefix    = values(prefix),
                mpn       = values(mpn),
                number    = values(number);
        """
        self.execute_without_results(statement, data, many=True, commit=True)

    def insert_into_specific_supplier_item(self, supplier, data: List[Dict]) -> None:
        if supplier == suppliers.Keystone:
            statement = """
                INSERT INTO keystone_item(supplier_item_id,
                                          LongDescription,
                                          JobberPrice,
                                          Cost,
                                          Fedexable,
                                          ExeterQty,
                                          MidWestQty,
                                          PacificNWQty,
                                          TexasQty,
                                          SouthEastQty,
                                          GreatLakesQty,
                                          CaliforniaQty,
                                          TotalQty,
                                          UPCCode,
                                          Prop65Toxicity,
                                          HazardousMaterial)
                  VALUES
                    (
                        (SELECT supplier_item_id
                           FROM
                             supplier_item
                           WHERE supplier_brand_id = %(supplier_brand_id)s
                             AND norm_mpn = %(norm_mpn)s
                        ),
                        %(LongDescription)s,
                        %(JobberPrice)s,
                        %(Cost)s,
                        %(Fedexable)s,
                        %(ExeterQty)s,
                        %(MidWestQty)s,
                        %(PacificNWQty)s,
                        %(TexasQty)s,
                        %(SouthEastQty)s,
                        %(GreatLakesQty)s,
                        %(CaliforniaQty)s,
                        %(TotalQty)s,
                        %(UPCCode)s,
                        %(Prop65Toxicity)s,
                        %(HazardousMaterial)s)
                  ON DUPLICATE KEY UPDATE
                    LongDescription   = values(LongDescription),
                    JobberPrice       = values(JobberPrice),
                    Cost              = values(Cost),
                    Fedexable         = values(Fedexable),
                    ExeterQty         = values(ExeterQty),
                    MidWestQty        = values(MidWestQty),
                    PacificNWQty      = values(PacificNWQty),
                    TexasQty          = values(TexasQty),
                    SouthEastQty      = values(SouthEastQty),
                    GreatLakesQty     = values(GreatLakesQty),
                    CaliforniaQty     = values(CaliforniaQty),
                    TotalQty          = values(TotalQty),
                    UPCCode           = values(UPCCode),
                    Prop65Toxicity    = values(Prop65Toxicity),
                    HazardousMaterial = values(HazardousMaterial);
            """
        elif supplier == suppliers.Meyer:
            statement = """
                INSERT INTO meyer_item(supplier_item_id, Jobber_Price, Customer_Price, UPC, MAP, 
                                       Weight, Height, Length, Width, Description, LTL_Eligible, Discontinued)
                  VALUES
                    (
                        (SELECT supplier_item_id
                           FROM
                             supplier_item
                           WHERE supplier_brand_id = %(supplier_brand_id)s
                             AND norm_mpn = %(norm_mpn)s
                        ),
                        %(Jobber_Price)s,
                        %(Customer_Price)s,
                        %(UPC)s,
                        %(MAP)s,
                        %(Weight)s,
                        %(Height)s,
                        %(Length)s,
                        %(Width)s,
                        %(Description)s,
                        %(LTL_Eligible)s,
                        %(Discontinued)s
                    )
                  ON DUPLICATE KEY UPDATE
                    Jobber_Price   = values(Jobber_Price),
                    Customer_Price = values(Customer_Price),
                    UPC            = values(UPC),
                    MAP            = values(MAP),
                    Weight         = values(Weight),
                    Height         = values(Height),
                    Length         = values(Length),
                    Width          = values(Width),
                    Description    = values(Description),
                    LTL_Eligible   = values(LTL_Eligible),
                    Discontinued   = values(Discontinued);
            """
        elif supplier == suppliers.Premier:
            statement = """
                INSERT INTO premier_item(supplier_item_id,
                                         Distributor_Cost,
                                         Package_Quantity,
                                         Core_Price,
                                         UPC,
                                         Part_Description,
                                         Inventory_Count,
                                         Inventory_Type)
                  VALUES
                    (
                        (SELECT supplier_item_id
                           FROM
                             supplier_item
                           WHERE supplier_brand_id = %(supplier_brand_id)s
                             AND norm_mpn = %(norm_mpn)s
                        ), 
                        %(Distributor_Cost)s,
                        %(Package_Quantity)s,
                        %(Core_Price)s,
                        %(UPC)s,
                        %(Part_Description)s,
                        %(Inventory_Count)s,
                        %(Inventory_Type)s
                    )
                  ON DUPLICATE KEY UPDATE
                    Distributor_Cost = values(Distributor_Cost),
                    Package_Quantity = values(Package_Quantity),
                    Core_Price       = values(Core_Price),
                    UPC              = values(UPC),
                    Part_Description = values(Part_Description),
                    Inventory_Count  = values(Inventory_Count),
                    Inventory_Type   = values(Inventory_Type);            
            """
        elif supplier == suppliers.Trans:
            statement = """
                INSERT INTO trans_item(supplier_item_id, CA, TX, FL, CO, OH, ID, PA, 
                                       LIST_PRICE, JOBBER_PRICE, TOTAL, STATUS)
                  VALUES
                    (
                        (SELECT supplier_item_id
                           FROM
                             supplier_item
                           WHERE supplier_brand_id = %(supplier_brand_id)s
                             AND norm_mpn = %(norm_mpn)s
                        ),
                        %(CA)s,
                        %(TX)s,
                        %(FL)s,
                        %(CO)s,
                        %(OH)s,
                        %(ID)s,
                        %(PA)s,
                        %(LIST_PRICE)s,
                        %(JOBBER_PRICE)s,
                        %(TOTAL)s,
                        %(STATUS)s)
                  ON DUPLICATE KEY UPDATE
                    CA           = values(CA),
                    TX           = values(TX),
                    FL           = values(FL),
                    CO           = values(CO),
                    OH           = values(OH),
                    ID           = values(ID),
                    PA           = values(PA),
                    LIST_PRICE   = values(LIST_PRICE),
                    JOBBER_PRICE = values(JOBBER_PRICE),
                    TOTAL        = values(TOTAL),
                    STATUS       = values(STATUS);
            """
        elif supplier == suppliers.Turn14:
            statement = """
                INSERT INTO turn14_item(supplier_item_id,
                                        Description,
                                        Cost,
                                        Retail,
                                        Jobber,
                                        CoreCharge,
                                        Map,
                                        Other,
                                        OtherName,
                                        EastStock,
                                        WestStock,
                                        CentralStock,
                                        Stock,
                                        MfrStock,
                                        MfrStockDate,
                                        DropShip,
                                        DSFee,
                                        Weight)
                  VALUES
                    (
                        (SELECT supplier_item_id
                           FROM
                             supplier_item
                           WHERE supplier_brand_id = %(supplier_brand_id)s
                             AND norm_mpn = %(norm_mpn)s
                        ),
                        %(Description)s,
                        %(Cost)s,
                        %(Retail)s,
                        %(Jobber)s,
                        %(CoreCharge)s,
                        %(Map)s,
                        %(Other)s,
                        %(OtherName)s,
                        %(EastStock)s,
                        %(WestStock)s,
                        %(CentralStock)s,
                        %(Stock)s,
                        %(MfrStock)s,
                        %(MfrStockDate)s,
                        %(DropShip)s,
                        %(DSFee)s,
                        %(Weight)s
                    )
                  ON DUPLICATE KEY UPDATE
                    Description  = values(Description),
                    Cost         = values(Cost),
                    Retail       = values(Retail),
                    Jobber       = values(Jobber),
                    CoreCharge   = values(CoreCharge),
                    Map          = values(Map),
                    Other        = values(Other),
                    OtherName    = values(OtherName),
                    EastStock    = values(EastStock),
                    WestStock    = values(WestStock),
                    CentralStock = values(CentralStock),
                    Stock        = values(Stock),
                    MfrStock     = values(MfrStock),
                    MfrStockDate = values(MfrStockDate),
                    DropShip     = values(DropShip),
                    DSFee        = values(DSFee),
                    Weight       = values(Weight);
            """
        else:
            raise TypeError('Wrong supplier')
        self.execute_without_results(statement, data, errorcode.ER_BAD_NULL_ERROR, many=True, commit=True)

    def insert_into_trans_supplier_item_from_file(self, data: List[Dict]) -> None:
        statement = """
            INSERT INTO trans_item(supplier_item_id, 
                                    DESCRIPTION,
                                    YOUR_PRICE,
                                    JOBBER_PRICE,
                                    MAP_CONFIRM_W_JOBBER,
                                    CORE_PRICE,
                                    FEDERAL_EXCISE_TAX, 
                                    OVERSIZE,
                                    STATUS)
              VALUES
                (
                    (SELECT supplier_item_id
                       FROM
                         supplier_item
                       WHERE supplier_brand_id = %(supplier_brand_id)s
                         AND norm_mpn = %(norm_mpn)s
                    ),
                    %(DESCRIPTION)s,
                    %(YOUR_PRICE)s,
                    %(JOBBER_PRICE)s,
                    %(MAP_CONFIRM_W_JOBBER)s,
                    %(CORE_PRICE)s,
                    %(FEDERAL_EXCISE_TAX)s,
                    %(OVERSIZE)s,
                    %(STATUS)s)
              ON DUPLICATE KEY UPDATE
                DESCRIPTION = values(DESCRIPTION),
                YOUR_PRICE = values(YOUR_PRICE),
                JOBBER_PRICE = values(JOBBER_PRICE),
                MAP_CONFIRM_W_JOBBER = values(MAP_CONFIRM_W_JOBBER),
                CORE_PRICE = values(CORE_PRICE),
                FEDERAL_EXCISE_TAX = values(FEDERAL_EXCISE_TAX),
                OVERSIZE = values(OVERSIZE),
                STATUS = values(STATUS);
        """
        self.execute_without_results(statement, data, errorcode.ER_BAD_NULL_ERROR, many=True, commit=True)

    def update_meyer_item__category_subcategory(self, data: List[Dict]) -> None:
        statement = """
            UPDATE meyer_item
            SET meyer_category_id    = %(meyer_category_id)s,
                meyer_subcategory_id = %(meyer_subcategory_id)s
            WHERE supplier_item_id = %(supplier_item_id)s;
        """
        self.execute_without_results(statement, data, many=True, commit=True)

    def update_meyer_item__inventory(self, data: List[Dict]) -> None:
        statement = """
            UPDATE meyer_item
              SET
                Qty_008          = %(Qty_008)s,
                Qty_032          = %(Qty_032)s,
                Qty_041          = %(Qty_041)s,
                Qty_044          = %(Qty_044)s,
                Qty_053          = %(Qty_053)s,
                Qty_062          = %(Qty_062)s,
                Qty_063          = %(Qty_063)s,
                Qty_065          = %(Qty_065)s,
                Qty_068          = %(Qty_068)s,
                Qty_069          = %(Qty_069)s,
                Qty_070          = %(Qty_070)s,
                Qty_071          = %(Qty_071)s,
                Qty_072          = %(Qty_072)s,
                Qty_077          = %(Qty_077)s,
                Qty_093          = %(Qty_093)s,
                Qty_094          = %(Qty_094)s,
                Qty_098          = %(Qty_098)s,
                Discontinued     = %(Discontinued)s
              WHERE supplier_item_id = %(supplier_item_id)s;
        """
        self.execute_without_results(statement, data, many=True, commit=True)

    def get_from_supplier_brand__name_and_supplier_id_2_supplier_brand_id(self) -> dict:
        cursor = self.connection.cursor(named_tuple=True)
        statement = """
            SELECT supplier_brand_id, name, supplier_id
              FROM
                supplier_brand;            
        """
        cursor.execute(statement)
        result = {}
        for row in cursor:
            result[(row.name, row.supplier_id)] = row.supplier_brand_id
        return result

    def get_supplier_number_2_supplier_item_id(self, supplier_id) -> dict:
        cursor = self.connection.cursor(named_tuple=True)
        statement = """
            SELECT si.number, si.supplier_item_id
              FROM
                supplier_item               si
                  INNER JOIN supplier_brand sb ON si.supplier_brand_id = sb.supplier_brand_id
              WHERE sb.supplier_id = %s AND si.available = TRUE;           
        """
        cursor.execute(statement, (supplier_id,))
        result = {}
        for row in cursor:
            if row.number in result:
                logging.warning('Duplicate number {}'.format(row.number))
            result[row.number] = row.supplier_item_id
        return result

    # noinspection SqlWithoutWhere

    def update_supplier_item_with_available(self) -> None:
        statement = """
            UPDATE supplier_item
              SET
                available = FALSE ;
        """
        self.execute_without_results(statement, many=False, commit=True)

    def write_turn14_item_info_from_get_items_api(
            self,
            p_supplier_item_id,
            p_item_id_in_api,
            p_product_name,
            p_category_name,
            p_subcategory_name,
            p_dimensions,
            p_thumbnail,
            p_barcode,
    ):
        cursor = self.connection.cursor()
        try:
            cursor.callproc(
                'write_turn14_item_info_from_get_items_api',
                (
                    p_supplier_item_id,
                    p_item_id_in_api,
                    p_product_name,
                    p_category_name,
                    p_subcategory_name,
                    p_dimensions,
                    p_thumbnail,
                    p_barcode,
                )
            )
            self.connection.commit()
        finally:
            cursor.close()

    def write_turn14_item_info_from_get_items_data_api(
            self,
            p_item_id_in_api,
            p_url,
            p_media_content,
            p_height,
            p_width,
    ):
        cursor = self.connection.cursor()
        try:
            cursor.callproc(
                'write_turn14_item_info_from_get_items_data_api',
                (
                    p_item_id_in_api,
                    p_url,
                    p_media_content,
                    p_height,
                    p_width,
                )
            )
            self.connection.commit()
        finally:
            cursor.close()

    def write_turn14_item_info_from_get_items_data_fitment_api(
            self,
            item_id_in_api,
            vehicle_fitments_ids
    ):
        cursor = self.connection.cursor()
        cursor.execute(
            """
                SELECT supplier_item_id
                FROM
                 turn14_item
                WHERE item_id_in_api = %s
            """,
            (item_id_in_api,)
        )
        item_id = cursor.fetchone()
        if item_id:
            item_id = item_id[0]
            cursor.executemany(
                """
                    INSERT IGNORE INTO turn14_fitment(turn14_item_id, vehicle_id)
                    VALUES
                    (%s, %s);
                """,
                [(item_id, vehicle_fitments_id) for vehicle_fitments_id in vehicle_fitments_ids]
            )
            self.connection.commit()
        cursor.close()

    def insert_into_meyer_item__item_information(self, data: List[Dict]) -> None:
        statement = """
            INSERT INTO meyer_item(
                supplier_item_id,
                Jobber_Price,
                Customer_Price,
                SuggestedRetailPrice,
                UPC,
                MAP,
                Weight,
                Height,
                Length,
                Width,  
                Description,  
                QtyAvailable, 
                Discontinued,  
                Kit,  
                Kit_Only,  
                LTL_Required,  
                Oversize
              )
              VALUES
                (
                   %(supplier_item_id)s,
                   %(Jobber_Price)s,
                   %(Customer_Price)s,
                   %(SuggestedRetailPrice)s,
                   %(UPC)s,
                   %(MAP)s,
                   %(Weight)s, 
                   %(Height)s, 
                   %(Length)s,  
                   %(Width)s,
                   %(Description)s,
                   %(QtyAvailable)s, 
                   %(Discontinued)s,  
                   %(Kit)s,  
                   %(Kit_Only)s,  
                   %(LTL_Required)s,
                   %(Oversize)s
                )
              ON DUPLICATE KEY UPDATE
                   Jobber_Price = values(Jobber_Price),
                   Customer_Price = values(Customer_Price),
                   SuggestedRetailPrice = values(SuggestedRetailPrice),
                   UPC = values(UPC),
                   MAP = values(MAP),
                   Weight = values(Weight), 
                   Height = values(Height), 
                   Length = values(Length),  
                   Width = values(Width),
                   Description = values(Description),
                   QtyAvailable = values(QtyAvailable), 
                   Discontinued = values(Discontinued),  
                   Kit = values(Kit),  
                   Kit_Only = values(Kit_Only),  
                   LTL_Required = values(LTL_Required),
                   Oversize = values(Oversize)
              ;
        """
        self.execute_without_results(statement, data, errorcode.ER_BAD_NULL_ERROR, many=True, commit=True)

    def insert_into_premier_item__pricing(self, data: List[Dict]) -> None:
        statement = """
            INSERT INTO premier_item(
                supplier_item_id, 
                cost_usd, 
                jobber_usd, 
                map_usd, 
                retail_usd, 
                cost_cad, 
                jobber_cad, 
                map_cad, 
                retail_cad
            )
              VALUES
                (
                    %(supplier_item_id)s, 
                    %(cost_usd)s, 
                    %(jobber_usd)s, 
                    %(map_usd)s, 
                    %(retail_usd)s, 
                    %(cost_cad)s, 
                    %(jobber_cad)s, 
                    %(map_cad)s, 
                    %(retail_cad)s
                )
              ON DUPLICATE KEY UPDATE
                cost_usd   = VALUES(cost_usd),
                jobber_usd = VALUES(jobber_usd),
                map_usd    = VALUES(map_usd),
                retail_usd = VALUES(retail_usd),
                cost_cad   = VALUES(cost_cad),
                jobber_cad = VALUES(jobber_cad),
                map_cad    = VALUES(map_cad),
                retail_cad = VALUES(retail_cad);
        """
        self.execute_without_results(statement, data, errorcode.ER_BAD_NULL_ERROR, many=True, commit=True)

    def insert_into_premier_item__inventory(self, data: List[Dict]) -> None:
        statement = """
            INSERT INTO premier_item(
                supplier_item_id,
                Qty_UT_1_US,
                Qty_KY_1_US,
                Qty_TX_1_US,
                Qty_CA_1_US,
                Qty_AB_1_CA,
                Qty_WA_1_US,
                Qty_CO_1_US,
                Qty_PO_1_CA
              )
              VALUES
                (
                    %(supplier_item_id)s,
                    %(Qty_UT_1_US)s,
                    %(Qty_KY_1_US)s,
                    %(Qty_TX_1_US)s,
                    %(Qty_CA_1_US)s,
                    %(Qty_AB_1_CA)s,
                    %(Qty_WA_1_US)s,
                    %(Qty_CO_1_US)s,
                    %(Qty_PO_1_CA)s 
                )
              ON DUPLICATE KEY UPDATE
                Qty_UT_1_US = VALUES(Qty_UT_1_US),
                Qty_KY_1_US = VALUES(Qty_KY_1_US),
                Qty_TX_1_US = VALUES(Qty_TX_1_US),
                Qty_CA_1_US = VALUES(Qty_CA_1_US),
                Qty_AB_1_CA = VALUES(Qty_AB_1_CA),
                Qty_WA_1_US = VALUES(Qty_WA_1_US),
                Qty_CO_1_US = VALUES(Qty_CO_1_US),
                Qty_PO_1_CA = VALUES(Qty_PO_1_CA);
        """
        self.execute_without_results(statement, data, errorcode.ER_BAD_NULL_ERROR, many=True, commit=True)

    def execute_without_results(
            self,
            statement: str,
            data=(),
            *allowable_error_codes,
            many: bool,
            commit: bool
    ) -> None:
        cursor = self.connection.cursor()
        try:
            if many:
                cursor.executemany(statement, data)
            else:
                cursor.execute(statement, data)
        except mysql.connector.Error as e:
            if e.errno in allowable_error_codes:
                logging.warning('Error: {} while \n{}'.format(e, cursor.statement))
            else:
                logging.error('Error: {} while \n{}'.format(e, cursor.statement))
                raise
        else:
            if commit:
                self.connection.commit()
        finally:
            cursor.close()

    def execute_with_results(
            self,
            statement: str,
            data=()
    ) -> MySQLCursorNamedTuple:
        cursor = self.connection.cursor(named_tuple=True)
        try:
            cursor.execute(statement, data)
        except mysql.connector.Error as e:
            logging.error('Error: {} while \n{}'.format(e, cursor.statement))
            raise
        else:
            return cursor


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    db = Database(
        create=True,
        option_files=constants.DATABASE_DB_CONFIG_FILE
    )
