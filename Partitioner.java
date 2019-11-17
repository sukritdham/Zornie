
private <T> List<List<T>> partitioner(List<T> list, int slots) throws Exception {

		if(list.size()<slots) {
			
			throw new Exception("Number of slots cannot exceed number of elements in list");
			
		}
			
			
		int bucketSize = 0;
		int bucketSizeForFinalList = 0;
		boolean unequalBucketSize = false;
		List<List<T>> listOfList = new ArrayList<>();
		List<T> temp = new ArrayList<>();
		

		bucketSize = list.size() / slots;

		if ((list.size() % slots) != 0) {
			unequalBucketSize = true;
			bucketSizeForFinalList = (list.size() / slots) + (list.size() % slots);
		}

		int finalListTracker = 1;
		int i = 0;
		int j = 0;


		while (i < list.size()) {


			if (j == bucketSize) {
				
				if (unequalBucketSize) {
					finalListTracker++;
					if (finalListTracker == slots) {
						bucketSize = bucketSizeForFinalList;

					}
				}
				listOfList.add(temp);
				temp = new ArrayList<>();
				j = 0;

			} else {
				temp.add(list.get(i));
				i++;
				j++;

			}
		}
		
		listOfList.add(temp);
		return listOfList;

}
