import logging
from bike_rental_db.collections import StationsCollection, TripsCollection, GroupedTripsCollection, StationsStatusCollection

# Basic logger configuration
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    handlers=[
        logging.StreamHandler()  
    ]
)

# Create the logger
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting data population process")
    
    try:
        # StationsCollection().populate()
        # logger.info("StationsCollection populated successfully")

        # StationsStatusCollection().populate()
        # logger.info("StationsStatusCollection populated successfully")

        # TripsCollection().populate()
        # logger.info("TripsCollection populated successfully")

        GroupedTripsCollection().populate()
        logger.info("GroupedTripsCollection populated successfully")
    
    except Exception as e:
        logger.error("Error during data population: %s", str(e))
        raise

    logger.info("Data population process completed")

if __name__ == "__main__":
    main()
