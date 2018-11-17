package alluxio.client.file.cache.metric;

import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.cache.*;
import alluxio.client.file.cache.submodularLib.cacheSet.CacheSet;
import alluxio.client.file.cache.submodularLib.cacheSet.DivideGR;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public enum ReadMetric {
	INSTANCE;
  public int unitNum;
  public int coincidenceNum;
  public long mUnMemorySize;
  public long mRemoteSize;
  public long mReadingSize;
  public LinkedList<MetricUnit> mMetricInfos = new LinkedList<>();
  private MetricUnit mCurrentUnit;
  private int mMoveLength = 1000;
  public int mAccessLength = 0;
  public double mCurrentCoincidenceRatio;
  public double unHitRatio = 0.2;
  public CacheManager mCacheManager;

  public interface levelPolicy {
  	public double getCoincidenceRatio();

  	public void switchCheck();

  	public levelPolicy policyUp();

  	public levelPolicy policyDown();
	}

	public synchronized void onAccess(CacheUnit cacheUnit) {
		if(!cacheUnit.isFinish()) {
			TempCacheUnit unit = (TempCacheUnit)cacheUnit;
		  double missRatio =  unit.getNewCacheSize() / unit.getSize();
		  if(missRatio > 0 && missRatio < 1 ) {
			  coincidenceNum ++;
				mCurrentUnit.setCoincidence(true);
		  }
		  unitNum = unitNum + 1 - unit.mCacheConsumer.size();
		}
		mMetricInfos.addLast(mCurrentUnit);
		mAccessLength ++;
		mCurrentUnit = new MetricUnit();
	}

	public void moveFormord() {
		int i = mMoveLength;
		while (i-- > 0 && mMetricInfos.size() > 0) {
			mAccessLength --;
			MetricUnit unit = mMetricInfos.pollFirst();
			mRemoteSize -= unit.getmRemoteSize();
			mUnMemorySize -= unit.getmUnMemorySize();
			mReadingSize -= unit.getmReadingSize();
			if(unit.isCoincidence()) {
				coincidenceNum --;
			}
			unit = null;
		}
	}

  /*
	public synchronized void metric(BlockInStream in, int read) {
		mReadingSize += read;
		mCurrentUnit.addReadingSize(read);
		switch (in.getSource()) {
			case LOCAL:
				if(!in.mTier.equals("MEM")) {
					mCurrentUnit.addUnMemorySize(read);
				}
				mUnMemorySize += read;
				break;
			case REMOTE:
				mCurrentUnit.addRemoteSize(read);
				mRemoteSize += read;
				break;
			default:
				break;
		}
	}*/

	public void Async2Sync(SKPolicy asyncPolicy, CachePolicy syncPolicy) {
		CacheSet input;
		if(asyncPolicy.useOne) {
			input = asyncPolicy.mInputSpace1;
		} else {
			input = asyncPolicy.mInputSpace2;
		}
		for (Map.Entry entry : input.sortCacheMap.entrySet()) {
			Queue<CacheUnit> q = (PriorityQueue<CacheUnit>) entry.getValue();
		  while(!q.isEmpty()) {
		  	CacheUnit tmp = q.poll();
				CacheInternalUnit currentUnit = (CacheInternalUnit) ClientCacheContext.INSTANCE.
					mFileIdToInternalList.get(tmp.getFileId()).getKeyFromBucket
					(tmp.getBegin(), tmp.getEnd(), null);
				syncPolicy.fliter(currentUnit, (BaseCacheUnit)tmp);
			}
		}
	}

	public void Sync2Async(SKPolicy asyncPolicy, CachePolicy syncpolicy) {
		if(syncpolicy instanceof DivideGR) {
      Queue<BaseCacheUnit> q = ((DivideGR)syncpolicy).hitRatioQueue;
      while(!q.isEmpty()) {
      	BaseCacheUnit tmp = q.poll();
				CacheInternalUnit currentUnit = (CacheInternalUnit) ClientCacheContext.INSTANCE.
					mFileIdToInternalList.get(tmp.getFileId()).getKeyFromBucket
					(tmp.getBegin(), tmp.getEnd(), null);
				asyncPolicy.fliter(currentUnit, tmp);
			}
		}
		else if(syncpolicy instanceof  LRUPolicy) {
			LinkedList<CacheInternalUnit> l = ((LRUPolicy)syncpolicy).mAccessRecords;
			while(l.isEmpty()) {
				CacheInternalUnit unit = l.poll();
				BaseCacheUnit newUnit = new BaseCacheUnit(unit.getFileId(), unit.getBegin(), unit.getEnd());
				syncpolicy.fliter(unit, newUnit);
			}
		}



		new Date().getTime();
	}

	class MetricUnit {
	  double mMissRatio;
	  int mUnMemorySize;
	  int mRemoteSize;
	  int mReadingSize;
	  boolean isCoincidence = false;

		public boolean isCoincidence() {
			return isCoincidence;
		}

		public void setCoincidence(boolean inCoincidence) {
			this.isCoincidence = inCoincidence;
		}

		public MetricUnit() {
	  	mMissRatio = 0;
	  	mUnMemorySize = 0;
	  	mReadingSize = 0;
	  	mReadingSize = 0;
		}

		public double getmMissRatio() {
			return mMissRatio;
		}

		public int getmReadingSize() {
			return mReadingSize;
		}

		public void addReadingSize(int mReadingSize) {
			this.mReadingSize += mReadingSize;
		}

		public void setmMissRatio(double mMissRatio) {
			this.mMissRatio = mMissRatio;
		}

		public int getmUnMemorySize() {
			return mUnMemorySize;
		}

		public void addUnMemorySize(int mUnMemorySize) {
			this.mUnMemorySize += mUnMemorySize;
		}

		public int getmRemoteSize() {
			return mRemoteSize;
		}

		public void addRemoteSize(int mRemoteSize) {
			this.mRemoteSize += mRemoteSize;
		}
	}
}
