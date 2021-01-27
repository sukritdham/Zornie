package com.fico.pricing.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
 
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Component;

import com.fico.pricing.configuration.IPricingConst;
//import com.fico.pricing.domain.pricingdb.DA.BaseRate;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class DiffReportUtils {
	
	private final static Logger logger = LoggerFactory.getLogger(DiffReportUtils.class);		
	

	BufferedReader br;

	
	public void diffReportMatrixFile(List<Map<String, ?>> newFile, List<Map<String, ?>> previousFile,
			List<String> columnsListToCompare, String diffReportLocation, String newFileName, String previousFileName)  {
		try {

		Map<String, List<Object>> h1 = convertListToMap(newFile);
		Map<String, List<Object>> h2 = convertListToMap(previousFile);
		Map<String, Object[]>[] containers = new HashMap[columnsListToCompare.size()];

		Set<String> uniqueMatrixNames = new LinkedHashSet(h1.get(IPricingConst.MATRIX_NAME)); 
//		System.out.println(uniqueMatrixNames);
		Map<String,Integer[]> newMatrixIndexes = new LinkedHashMap<>();
		Map<String,Integer[]> prevMatrixIndexes = new LinkedHashMap<>();
		int newStartIndex=0,newEndIndex=0;
		int previousStartIndex=0,previousEndIndex=0;
		
		for (String s : uniqueMatrixNames) {
			
//			System.out.println("matrix-name:"+s);
//			System.out.println("newStartIndex:"+newStartIndex);
//			System.out.println("previousStartIndex:"+previousStartIndex);
			
			
			newEndIndex = newEndIndex+Collections.frequency(h1.get(IPricingConst.MATRIX_NAME), s);
			int newEndIndexCopy=newEndIndex;
			newEndIndexCopy--;
			previousEndIndex = previousEndIndex+Collections.frequency(h2.get(IPricingConst.MATRIX_NAME), s);
			int previousEndIndexCopy=previousEndIndex;
			previousEndIndexCopy--;
//			System.out.println("newEndIndexCopy:"+newEndIndexCopy);
//			System.out.println("previousEndIndexCopy:"+previousEndIndexCopy);
			newMatrixIndexes.put(s,new Integer[]{newStartIndex, newEndIndexCopy});
			prevMatrixIndexes.put(s,new Integer[]{previousStartIndex, previousEndIndexCopy});
			
			newStartIndex = newStartIndex+Collections.frequency(h1.get(IPricingConst.MATRIX_NAME), s);
			previousStartIndex = previousStartIndex+Collections.frequency(h2.get(IPricingConst.MATRIX_NAME), s);

		}
		
		//System.out.println(newMatrixIndexes);
		//System.out.println(prevMatrixIndexes);
		
		
		
		
		int containerCount = 0;
		 
		 List<Integer> newMatrixStartIndex = this.returnStartIndexList(newMatrixIndexes);
		 List<Integer> previousMatrixStartIndex = this.returnStartIndexList(prevMatrixIndexes);
		 

		for (String indexValue : columnsListToCompare) {

			List<List<Object>> newListOfLists = partitionList(h1.get(indexValue),newMatrixIndexes);
			List<List<Object>> previousListOfLists = partitionList(h2.get(indexValue),prevMatrixIndexes);
			Map<String, Object[]> rowContainers = new LinkedHashMap<>();
			Map<String, Object[]> rowContainersTemp = new LinkedHashMap<>();
			//System.out.println(previousListOfLists);
			List<Object[]> objList=new ArrayList<>();
//			Iterator<List<Object>> newListIterator = newListOfLists.iterator();
//			Iterator<List<Object>> prevListIterator = previousListOfLists.iterator();
			//int rowContainerCount=0;
			 boolean isHeaderRqd=true;
			 int indexCount=0;
	
			  List<String> matrixNamesList = new ArrayList(uniqueMatrixNames);
			 //System.out.println(matrixNamesList);
			 
			while(indexCount < (newListOfLists.size())) {
	
//				System.out.println("newMatrixStartIndex.get(indexCount)==>"+newMatrixStartIndex.get(indexCount)+" for:"+indexValue);
//				System.out.println("previousMatrixStartIndex.get(indexCount==>)"+previousMatrixStartIndex.get(indexCount)+" for:"+indexValue);
//				System.out.println("matrixNamesList.get(indexCount)==>)"+matrixNamesList.get(indexCount)+" for:"+indexValue);
				rowContainersTemp = listCompareMergeMatrixFile(indexValue + "-NEW", newListOfLists.get(indexCount),
							indexValue + "-OLD",previousListOfLists.get(indexCount), isHeaderRqd,newMatrixStartIndex.get(indexCount) ,previousMatrixStartIndex.get(indexCount),matrixNamesList.get(indexCount));
				
				rowContainers.putAll(rowContainersTemp);
				isHeaderRqd=false;
					
					//containerCount++;	
					//rowContainerCount++;
				indexCount++;
			}
			
			//System.out.println(rowContainers);
//			System.out.println("Row Container:"+rowContainers);
			containers[containerCount] = rowContainers;
			containerCount++;	
			//indexCount++;
			//System.out.println(containers);

		}

		

		Map<String, Object[]> tempContainer = containers[0];

		for (int i = 0; i + 1 < containers.length; i++) {

			tempContainer = mergeMapsMatrixFile(tempContainer, containers[i + 1]);

		}
		
//		System.out.println(tempContainer);
		writeExcelReport(tempContainer,diffReportLocation, newFileName, previousFileName);
		}catch(Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage(),ex);
			logger.info("Continue after diffreport error");
		}

	}
	

	public void diffReport(List<Map<String, ?>> newFile, List<Map<String, ?>> previousFile,
			List<String> columnsListToCompare, String diffReportLocation, String newFileName, String previousFileName)  {
		try {
		Map<String, List<Object>> h1 = convertListToMap(newFile);
		Map<String, List<Object>> h2 = convertListToMap(previousFile);
		Map<String, Object[]>[] containers = new HashMap[columnsListToCompare.size()];

		int containerCount = 0;

		for (String indexValue : columnsListToCompare) {

			containers[containerCount] = listCompareMerge(indexValue + "-NEW", h1.get(indexValue),
					indexValue + "-OLD", h2.get(indexValue));

			containerCount++;
		}

		// System.out.println(containers.length);

		Map<String, Object[]> tempContainer = containers[0];

		for (int i = 0; i + 1 < containers.length; i++) {

			tempContainer = mergeMaps(tempContainer, containers[i + 1]);

		}

		writeExcelReport(tempContainer,diffReportLocation, newFileName, previousFileName);
		}catch(Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage(),ex);
			logger.info("Continue after diffreport error");
		}

	}
	
private Map<String,Object[]> listCompareMerge(String leftColumnName,List<?> left, String rightColumnName, List<?> right) {

	
//		System.out.println("leftColumnName:"+leftColumnName);
//		System.out.println("left:"+left);
//		System.out.println("rightColumnName:"+rightColumnName);
//		System.out.println("right:"+right);
		
		Map<String, Object[]> diff = new HashMap<String, Object[]>();
		
		Object[] custObjDiff = new Object[3];
		
		custObjDiff[0]="RowNo";
		custObjDiff[1]=leftColumnName;
		custObjDiff[2]=rightColumnName;
		diff.put("Header", custObjDiff);
		
		for (int i = 0; i < ((left.size() == right.size()) ? left.size()
				: ((left.size() < right.size()) ? left.size() : right.size())); i++) {

//			System.out.println("left.get(i):"+left.get(i));
//			System.out.println("right.get(i):"+right.get(i));
//			System.out.println("Comparison result:"+left.get(i).equals(right.get(i)));
			
			if (null != left && null != right && !left.get(i).equals(right.get(i))) {

				custObjDiff = new Object[3];
				custObjDiff[0]=i + 1;
				custObjDiff[1] = left.get(i);
				custObjDiff[2]= right.get(i);
				diff.put(String.valueOf(i+1), custObjDiff);
	

			}
		}

		if (left.size() == right.size()) {

		//	System.out.println("Lists are equal");

		} else if (left.size() > right.size()) {

		//	System.out.println("Left list has more elements");

			for (int x = (right.size()); x < left.size(); x++) {


				
				custObjDiff = new Object[3];
				custObjDiff[0]=x + 1;
				custObjDiff[1] = left.get(x);
				custObjDiff[2]= null;
				
				diff.put(String.valueOf(x+1), custObjDiff);

			}

		} else {

	//		System.out.println("Right list has more elements");

			for (int x = (left.size()); x < right.size(); x++) {


				custObjDiff = new Object[3];
				custObjDiff[0]= x + 1;
				custObjDiff[1] = null;
				custObjDiff[2]= right.get(x);
				
				diff.put(String.valueOf(x+1), custObjDiff);
			}

		}
	
		
		return diff;

	}

private Map<String,Object[]> listCompareMergeMatrixFile(String leftColumnName,List<?> left, String rightColumnName, List<?> right,boolean isHeaderReqd, int newListStartIndex, int previousListStartIndex,String matrixName) {

	
//	System.out.println("leftColumnName:"+leftColumnName);
//	System.out.println("left:"+left);
//	System.out.println("rightColumnName:"+rightColumnName);
//	System.out.println("right:"+right);
	
	Map<String, Object[]> diff = new HashMap<String, Object[]>();
	
	Object[] custObjDiff = new Object[4];
	
	custObjDiff[0]="RowNo";
	custObjDiff[1]="MatrixName";
	custObjDiff[2]=leftColumnName;
	custObjDiff[3]=rightColumnName;
	
	if(isHeaderReqd)
		diff.put("Header", custObjDiff);
	
	for (int i = 0; i < ((left.size() == right.size()) ? left.size()
			: ((left.size() < right.size()) ? left.size() : right.size())); i++) {

//		System.out.println("left.get(i):"+left.get(i));
//		System.out.println("right.get(i):"+right.get(i));
//		System.out.println("Comparison result:"+left.get(i).equals(right.get(i)));
		
		if (null != left && null != right && !left.get(i).equals(right.get(i))) {

			custObjDiff = new Object[4];
			custObjDiff[0]= newListStartIndex+i + 1;
			custObjDiff[1]=matrixName;
			custObjDiff[2] = left.get(i);
			custObjDiff[3]= right.get(i);
			diff.put(String.valueOf(newListStartIndex+i+1), custObjDiff);


		}
	}

	if (left.size() == right.size()) {

	//	System.out.println("Lists are equal");

	} else if (left.size() > right.size()) {

	//	System.out.println("Left list has more elements");

		for (int x = (right.size()); x < left.size(); x++) {


			
			custObjDiff = new Object[4];
			custObjDiff[0]=newListStartIndex+x + 1;
			custObjDiff[1]=matrixName;
			custObjDiff[2] = left.get(x);
			custObjDiff[3]= null;
			
			diff.put(String.valueOf(newListStartIndex+x+1), custObjDiff);

		}

	} else {

//		System.out.println("Right list has more elements");

		for (int x = (left.size()); x < right.size(); x++) {


			custObjDiff = new Object[4];
			custObjDiff[0]= previousListStartIndex+x + 1;
			custObjDiff[1]=matrixName;
			custObjDiff[2] = null;
			custObjDiff[3]= right.get(x);
			
			diff.put(String.valueOf(previousListStartIndex+x+1), custObjDiff);
		}

	}

	
	return diff;

}


	private Map<String,Object[]> mergeMaps(Map<String,Object[]> left, Map<String,Object[]> right){
	
		int leftLength=0;
		int rightLength=0;
		Set<Entry<String, Object[]>> leftEntrySet = left.entrySet();
		Set<Entry<String, Object[]>> rightEntrySet = right.entrySet();
		
		for(Map.Entry<String,Object[]> kv:leftEntrySet) {
			leftLength=kv.getValue().length;
			break;
		}
		
		for(Map.Entry<String,Object[]> kv:rightEntrySet) {
			rightLength=kv.getValue().length;
			break;
		}

		Map<String,Object[]> mergedMap = new HashMap<String,Object[]>();
		MapDifference<String,Object[]> diff =  Maps.difference(left, right);
//		System.out.println("common:"+diff.entriesInCommon());
//		System.out.println("diff:"+diff.entriesDiffering());
//		System.out.println("left entries:"+diff.entriesOnlyOnLeft());
//		System.out.println("right entries:"+diff.entriesOnlyOnRight());
		
		Set<String> differingSet=diff.entriesDiffering().keySet();
		Set<String> entriesOnlyOnLeftSet=diff.entriesOnlyOnLeft().keySet();
		Set<String> entriesOnlyOnRightSet=diff.entriesOnlyOnRight().keySet();

		
		for(String key:differingSet) {
			
			Object[] obj1 = left.get(key);
			Object[] obj2 = right.get(key);
			int fal = obj1.length;         
			int sal = obj2.length-1;
			Object[] result = new Object[fal + sal];    
			System.arraycopy(obj1, 0, result, 0, fal);  
			System.arraycopy(obj2, 1, result, fal, sal);  
			mergedMap.put(key, result);
		}
		
		for(String key:entriesOnlyOnLeftSet) {
			
			
			Object[] obj2 = new Object[rightLength];
			Object[] obj1 = left.get(key);
			int fal = obj1.length;         
			int sal = obj2.length-1;
			Object[] result = new Object[fal + sal];    
			System.arraycopy(obj1, 0, result, 0, fal);  
			System.arraycopy(obj2, 1, result, fal, sal);  
			mergedMap.put(key, result);
		}
		
		for(String key:entriesOnlyOnRightSet) {
			
			Object[] obj1 = new Object[leftLength];
			Object[] obj2 = right.get(key);
			obj1[0]=key;
			int fal = obj1.length;         
			int sal = obj2.length-1;
			Object[] result = new Object[fal + sal];    
			System.arraycopy(obj1, 0, result, 0, fal);  
			System.arraycopy(obj2, 1, result, fal, sal); 
			mergedMap.put(key, result);
		}

		return mergedMap;
	}
	
	private Map<String,Object[]> mergeMapsMatrixFile(Map<String,Object[]> left, Map<String,Object[]> right){
		
		int leftLength=0;
		int rightLength=0;
		Set<Entry<String, Object[]>> leftEntrySet = left.entrySet();
		Set<Entry<String, Object[]>> rightEntrySet = right.entrySet();
		
		for(Map.Entry<String,Object[]> kv:leftEntrySet) {
			leftLength=kv.getValue().length;
			break;
		}
		
		for(Map.Entry<String,Object[]> kv:rightEntrySet) {
			rightLength=kv.getValue().length;
			break;
		}

		Map<String,Object[]> mergedMap = new HashMap<String,Object[]>();
		MapDifference<String,Object[]> diff =  Maps.difference(left, right);
//		System.out.println("common:"+diff.entriesInCommon());
//		System.out.println("diff:"+diff.entriesDiffering());
//		System.out.println("left entries:"+diff.entriesOnlyOnLeft());
//		System.out.println("right entries:"+diff.entriesOnlyOnRight());
		
		Set<String> differingSet=diff.entriesDiffering().keySet();
		Set<String> entriesOnlyOnLeftSet=diff.entriesOnlyOnLeft().keySet();
		Set<String> entriesOnlyOnRightSet=diff.entriesOnlyOnRight().keySet();

		
		for(String key:differingSet) {
			
			Object[] obj1 = left.get(key);
			Object[] obj2 = right.get(key);
			int fal = obj1.length;         
			int sal = obj2.length-1;
			Object[] result = new Object[fal + sal-1];    
			System.arraycopy(obj1, 0, result, 0, fal);  
			System.arraycopy(obj2, 2, result, fal, sal-1);  
			mergedMap.put(key, result);
		}
		
		for(String key:entriesOnlyOnLeftSet) {
			
			
			Object[] obj2 = new Object[rightLength];
			Object[] obj1 = left.get(key);
			int fal = obj1.length;         
			int sal = obj2.length-1;
			Object[] result = new Object[fal + sal-1];    
			System.arraycopy(obj1, 0, result, 0, fal);  
			System.arraycopy(obj2, 2, result, fal, sal-1); 
			mergedMap.put(key, result);
		}
		
		for(String key:entriesOnlyOnRightSet) {
			
			Object[] obj1 = new Object[leftLength];
			Object[] obj2 = right.get(key);
			obj1[0]=key;
			obj1[1]=obj2[1];
			int fal = obj1.length;         
			int sal = obj2.length-1;
			Object[] result = new Object[fal + sal-1];    
			System.arraycopy(obj1, 0, result, 0, fal);  
			System.arraycopy(obj2, 2, result, fal, sal-1); 
			mergedMap.put(key, result);
		}

		return mergedMap;
	}
	
	
	
	public Map<String, List<Object>> convertListToMap(List<? extends Map<String, ?>> inputList) {

		Map<String, List<Object>> hm = new HashMap<String, List<Object>>();
		List<Object> listTemp = new ArrayList<>();

		for (Map<String, ?> m : inputList) {

			Set<String> set = m.keySet();
			String[] s = new String[set.size()];
			set.toArray(s);

			for (int i = 0; i < s.length; i++) {
				listTemp = new ArrayList<>();

				if (hm.get(s[i]) == null) {
					listTemp.add(m.get(s[i]));
					hm.put(s[i], listTemp);
				} else {

					hm.get(s[i]).add(m.get(s[i]));

				}
			}
		}

		return hm;
	}

private List<Integer> returnStartIndexList(Map<String, Integer[]> input){
		
		List<Integer> returnList=new ArrayList<>();
		
		for(Integer[] x:input.values()) {
			
			returnList.add(x[0]);
			
		}
		
		
		return returnList;
		
		
		
	}
	
	private List<List<Object>> partitionList(List<Object> input, Map<String,Integer[]> matrixIndexes){
		
		
		Set<String> keyList=matrixIndexes.keySet();
		List<List<Object>> returnList=new ArrayList<>();
		
		
		for(String k:keyList) {
			List<Object> tempInput=new ArrayList<>();
			//System.out.println(k);
			int startIndex = matrixIndexes.get(k)[0];
			int endIndex = matrixIndexes.get(k)[1];
			tempInput = input.subList(startIndex, endIndex+1);
			returnList.add(tempInput);
		}

		return returnList;
	}
	
	
	private void writeExcelReport(Map<String,Object[]> data, String outputPath,String newFileName, String previousFileName) { 
        // Blank workbook 
		
        XSSFWorkbook workbook = new XSSFWorkbook(); 
  
        // Create a blank sheet 
        XSSFSheet sheet = workbook.createSheet("PricingDiffReport"); 

        // This data needs to be written (Object[]) 
//        Map<String, Object[]> data = new TreeMap<String, Object[]>(); 
//        data.put("1", new Object[]{ "ID", "NAME", "LASTNAME" }); 
//        data.put("2", new Object[]{ 1, "abc", "def" }); 
//        data.put("3", new Object[]{ 2, "xyz", "pqr" }); 
//        data.put("4", new Object[]{ 3, "ghi", "jkl" }); 
//        data.put("5", new Object[]{ 4, "mno", "tuv" }); 
  
        // Iterate over data and write to sheet 
        
//        Set<String> colKeyset = container.keySet(); 
//        for (String colName : colKeyset) { 
//            // this creates a new row in the sheet 
//
//            System.out.println(colName);
//        } 
        
        int rownum = 0; 
        Row row = sheet.createRow(rownum++); 
        Object[] objArr = data.get("Header"); 
        int cellnum = 0; 
        for (Object obj : objArr) { 
            // this line creates a cell in the next column of that row 
            Cell cell = row.createCell(cellnum++); 
            if (obj instanceof String) 
                cell.setCellValue((String)obj); 
 
        } 
    
        Set<String> keyset = data.keySet(); 
        rownum = 1; 
        for (String key : keyset) { 
            // this creates a new row in the sheet
          if(null!=key && !key.equals("Header")) {
            row = sheet.createRow(rownum++); 
            objArr = data.get(key); 
            cellnum = 0; 
            for (Object obj : objArr) { 
                // this line creates a cell in the next column of that row 
                Cell cell = row.createCell(cellnum++); 
                if (obj instanceof String) 
                    cell.setCellValue((String)obj); 
                else if (obj instanceof Integer) 
                    cell.setCellValue((Integer)obj); 
                else if (obj instanceof Boolean) 
                    cell.setCellValue((Boolean)obj); 
                else if (obj instanceof Byte) 
                    cell.setCellValue((Byte)obj); 
                else if (obj instanceof Character) 
                    cell.setCellValue((Character)obj); 
                else if (obj instanceof Short) 
                    cell.setCellValue((Short)obj); 
                else if (obj instanceof Long) 
                    cell.setCellValue((Long)obj); 
                else if (obj instanceof Float) 
                    cell.setCellValue((Float)obj);
                else if (obj instanceof Double) 
                    cell.setCellValue((Double)obj);
                else if (obj instanceof Date) 
                    cell.setCellValue(new SimpleDateFormat("MM/dd/yyyy").format((Date)(obj)));
                
            } 
          }
        } 
        
       
        try { 
            // this Writes the workbook excelReport 
        	//logic expects 
        	String diffReportFileName = IPricingConst.DIFF_PREFIX +"_"+newFileName.split("\\.")[0]+"_"+previousFileName.split("\\.")[0]+"_"+System.currentTimeMillis()+".xlsx";
            FileOutputStream out = new FileOutputStream(new File(outputPath+diffReportFileName)); 
            workbook.write(out); 
            out.close(); 
            System.out.println(diffReportFileName + " written successfully to disk at "+ outputPath); 
        } 
        catch (Exception e) { 
            e.printStackTrace(); 
        } 
    }
	
	
	
} 


