import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.DefaultTransactionDefinition;

/*
This class is a Spring batch writer that works to perform transactional DB inserts/updates across data centers in a transactional manner.
The insertIntoDB method is significant in this regard and uses Spring's PlatformTransactionManager for transaction management.
*/

@Transactional(propagation = Propagation.NOT_SUPPORTED)
public class PricingGridWriter implements StepExecutionListener, ItemWriter<Map<String, ?>> {

	private final static Logger logger = LoggerFactory.getLogger(PricingGridWriter.class);

	@Autowired
	private ProductProfileUtils productProfileUtils;

	@Autowired
	private ProductProfileProperties productProfileProperties;

	@Autowired
	@Qualifier("envlist")
	Map<String, DataSource> dataSourceByEnv;

	private String fileName;

	private int chunkSize;

	Object effDate;

	String env;
	String jobExId;
	
	boolean isLegacy; 
	
	@Autowired
	PricingGridDB pgdb;

	@Override
	public void write(List<? extends Map<String, ?>> items) throws Exception {
		// TODO Auto-generated method stub
		logger.info("IN WRITER ==============");

		// System.out.println(items);
//		FileNamePatternBean fileNamePatternBeanPREV = new FileNamePatternBean(
//				fileNamePatternBean.getProductName() + "_" + streamName + "_" + "PREVIOUS.csv");
//		fileNamePatternBeanPREV.setObjectType(streamName);
//		FileNamePatternBean fileNamePatternBeanOfPreviousFile = new FileNamePatternBean(
//	"DA_BASERATE_PREVIOUS.csv");
//		List<Map<String, ?>> responseListOfOLDMap = beanIOValidator.process(fileNamePatternBeanPREV);
//		diffReportUtils.diffReport(items, responseListOfOLDMap,
//				productProfileUtils.getProductProfileForFileName(fileName)
//						.getDiffreportcomparisoncolumnlist(),
//				productProfileUtils.getProductProfileForFileName(fileName).getDiffreportlocation(),
//				fileName, fileNamePatternBeanPREV.getFileName());
		items = new ArrayList<>(items);
		logger.info("About to delete 0th element i.e:" + items.get(0));
		items.remove(0); // remove/skip header entry
		logger.info("Total records after removal of header:" + items.size());
		insertIntoDB(items.stream().toArray(HashMap[]::new));
		logger.info("FINISHED INSERT TO DB");
	}

	private void insertIntoDB(Map<String, Object>[] mapList) throws Exception {

		FileNamePatternBean fileNamePatternBean = new FileNamePatternBean(fileName);

		String insertSQL = productProfileUtils.getProductProfileForFileName(fileNamePatternBean.getGridName())
				.getInsertSQL();
		String deleteSQL = productProfileUtils.getProductProfileForFileName(fileNamePatternBean.getGridName())
				.getDeleteSQL(); // to-do
		String updateSQL = productProfileUtils.getProductProfileForFileName(fileNamePatternBean.getGridName())
				.getUpdateSQL();
		

		logger.debug("Env " + env);

		String aDBList[] = productProfileProperties.getEnvlabel().get(env).toString().split(",");
		logger.info("aDBList=======>>>>>>" + (Arrays.asList(aDBList)));

		logger.info("Logic for transactional update,delete & insert operations for LEGACY GRIDS");

		List<TransactionAttributePair> listForXactionAttribPair = new ArrayList<>();
		DefaultTransactionDefinition def = new DefaultTransactionDefinition();
		PlatformTransactionManager txManager = null;
		TransactionStatus status = null;
		String dbURL = new String();
		// explicitly setting the transaction name is something that can only be done
		// programmatically
		def.setName(jobExId);
		//def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);

		for (String dbenvKey : aDBList) {

			TransactionAttributePair txAttribPair = new TransactionAttributePair();
			logger.info("Inside data source transactional processing loop");
			DataSource ds = dataSourceByEnv.get(dbenvKey);

			try {
				dbURL = ds.getConnection().getMetaData().getURL();
				logger.info("DB URL::" + ds.getConnection().getMetaData().getURL());
				txManager = new DataSourceTransactionManager(ds);
				status = txManager.getTransaction(def);
				txAttribPair.setPlatformTransactionManager(txManager);
				txAttribPair.setStatus(status);
				listForXactionAttribPair.add(txAttribPair);
				
				NamedParameterJdbcTemplate namedParamJdbcTemplate = new NamedParameterJdbcTemplate(ds);
				Date currEffDate = (Date) mapList[0].get(IPricingConst.EFF_DATE);
				
				Date nextEffDate = getNextEffDate(namedParamJdbcTemplate, fileNamePatternBean.getGridName(), currEffDate);
				
				setEndDate(mapList,nextEffDate);
				
				SqlParameterSource[] batch = SqlParameterSourceUtils.createBatch(mapList);

				
				logger.info("BUIZ LOGIC START");
				// previous records logic SET END_DATE = :EFFECTIVE_DATE where END_DATE=
				// '31-DEC-2099'
				int updatedRecordsCount = namedParamJdbcTemplate.update(updateSQL, mapList[0]);
				logger.info("total updatedRecordsCount:" + updatedRecordsCount);
				// delete records with same effective date
				int deletedRecordsCount = namedParamJdbcTemplate.update(deleteSQL, mapList[0]);
				logger.info("total deletedRecordsCount:" + deletedRecordsCount);
				// insert new records
				int[] insertedRecordsCount = namedParamJdbcTemplate.batchUpdate(insertSQL, batch);
				logger.info("total insertedRecordsCount:" + insertedRecordsCount.length);
				
				effDate = mapList[0].get(IPricingConst.EFF_DATE);
				
				if(isLegacy) {
					Map<String, Object>[] clnMapList = mapList.clone();
					Date legacyDate = pgdb.getLegacyDate(env);
					logger.info("isLegacy DB Update " + isLegacy + ":" + legacyDate );
					if(legacyDate !=null) {
						
						nextEffDate = getNextEffDate(namedParamJdbcTemplate, fileNamePatternBean.getGridName(), legacyDate);
						
						setEffDate(clnMapList,legacyDate);
						setEndDate(clnMapList,nextEffDate);
						
						batch = SqlParameterSourceUtils.createBatch(clnMapList);

						
						logger.info("BUIZ LOGIC START");
						// previous records logic SET END_DATE = :EFFECTIVE_DATE where END_DATE=
						// '31-DEC-2099'
						updatedRecordsCount = namedParamJdbcTemplate.update(updateSQL, clnMapList[0]);
						logger.info("total isLegacy updatedRecordsCount:" + updatedRecordsCount);
						// delete records with same effective date
						deletedRecordsCount = namedParamJdbcTemplate.update(deleteSQL, clnMapList[0]);
						logger.info("total isLegacy deletedRecordsCount:" + deletedRecordsCount);
						// insert new records
						insertedRecordsCount = namedParamJdbcTemplate.batchUpdate(insertSQL, batch);
						logger.info("total isLegacy insertedRecordsCount:" + insertedRecordsCount.length);
					}else {
						throw new PGException("Legacy date is not configured in the data base, use the updateLegacyDate rest service to insert record");
					}
				}

				//effDate = mapList[0].get(IPricingConst.EFF_DATE);

				logger.info("BUIZ LOGIC END");

			} catch (Exception ex) {
				logger.error("Exception thrown for dbURL:" + dbURL);
				ex.printStackTrace();
				for (TransactionAttributePair txAttPair : listForXactionAttribPair) {
					// Rollback all DB-Opns in this env
					logger.error(ex.getMessage());
					logger.info("DB RT Exception thrown  .. Before calling Rollback");
					txAttPair.getPlatformTransactionManager().rollback(txAttPair.getStatus());
					logger.info("DB RT Exception thrown  .. After calling Rollback");
				}
				// break;
				throw ex;
			}

		}

		logger.info("Finished data source transactional processing loop");

		for (TransactionAttributePair txAttPair : listForXactionAttribPair) {

			// Commit all DB-Opns in this env
			logger.info("Before calling COMMIT");
			txAttPair.getPlatformTransactionManager().commit(txAttPair.getStatus());
			logger.info("After calling COMMIT");

		}

		// }

		/*
		 * update where endDate = 31-12-2099 set endDate (newEffectiveDate) insert
		 * effectiveDate, 31-12-2099, x, x, x
		 * 
		 * Just CQA get min value where effectiveDate > cqaEffectiveDate insert
		 * cqaEffectiveDate, queryDate, x, x, x
		 */

		logger.info("AFTER SQL invocations");
	}
	
	public void setEndDate(Map<String, Object>[] mapList, Date endDate) {
		logger.debug("prev setEndDate " + endDate);
		for (Map<String, Object> map : mapList) {
			map.put(IPricingConst.END_DATE, endDate);
		//	logger.debug(map.toString());
		}
	}
	
	public void setEffDate(Map<String, Object>[] mapList, Date effDate) {
		logger.debug("prev setEffDate " + effDate);
		for (Map<String, Object> map : mapList) {
			map.put(IPricingConst.EFF_DATE, effDate);
		}
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		// TODO Auto-generated method stub
		isLegacy = false;
		logger.info("beforeStep PricingGridWriter " + stepExecution.getJobParameters() + ":"
				+ stepExecution.getJobExecution().getExecutionContext() + ":" + isLegacy);
		this.fileName = stepExecution.getJobExecution().getExecutionContext()
				.getString(IPricingConst.CSV_CURR_FILENAME);
		env = stepExecution.getJobParameters().getString(IPricingConst.ENV);
		jobExId = stepExecution.getJobExecutionId().toString();
		
		String LegacyDateConfig= productProfileProperties.getEnvconfig().get(env).getLoadlegacydate();
		if("TRUE".equalsIgnoreCase(LegacyDateConfig)) {
			isLegacy = true;
		}
		logger.info("beforeStep end PricingGridWriter " + LegacyDateConfig + ":" + isLegacy);
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		// TODO Auto-generated method stub
		logger.info("AFTER STEP " + effDate);
		stepExecution.getJobExecution().getExecutionContext().put(IPricingConst.EFF_DATE, effDate);
		logger.info("AFTER STEP COMPLETED");
		return ExitStatus.COMPLETED;

	}
	
	public Date getNextEffDate(NamedParameterJdbcTemplate namedParamJdbcTemplate, String gridName, Date currEffDate) {
		logger.debug("getNextEffDate of " + currEffDate);
		Date mEffDate = null; 
		try {
			String nextEffSQL = productProfileUtils.getProductProfileForFileName(gridName).getMaxEffSQL();

			Map params =new HashMap();
			params.put(IPricingConst.EFF_DATE, currEffDate);  
			
			Map responseMap = namedParamJdbcTemplate.queryForMap(nextEffSQL, params);
			//java.sql.Date dobj = (java.sql.Date) params.get(IPricingConst.MAX_EFF_DATE);
			logger.debug("getNextEffDate Response " + responseMap );
			mEffDate = PricingGridDB.getJavaDate(responseMap.get(IPricingConst.MAX_EFF_DATE));
		} catch (Exception e) {
			throw new PGException("getNextEffDate failed " , e);
		}
		return mEffDate;
	}
	
	
	

}
 
