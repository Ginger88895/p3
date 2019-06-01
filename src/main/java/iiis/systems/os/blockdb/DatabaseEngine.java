package iiis.systems.os.blockdb;

import java.util.HashMap;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;
import java.io.IOException;

public class DatabaseEngine
{
    private static DatabaseEngine instance=null;

    public static DatabaseEngine getInstance()
	{
        return instance;
    }

    public static void setup(String dataDir)
	{
        instance=new DatabaseEngine(dataDir);
    }

    private HashMap<String,Integer> balances=new HashMap<>();
    private int logLength;
    private String dataDir;
	private String transientDir;

	private int blockSize;
	private int blockID;
	private boolean recovered;
	private JSONObject pendingTrans[];

    DatabaseEngine(String dataDir)
	{
        this.dataDir=dataDir;
		this.logLength=0;
		//N=50
		this.blockSize=50;
		this.blockID=1;
		this.recovered=false;
		this.pendingTrans=new JSONObject[this.blockSize*10];
		this.transientDir="transient/";
		recover();
    }

    private int getOrZero(String userID)
	{
		if(balances.containsKey(userID))
			return balances.get(userID);
		else
			return 0;
    }

    public int get(String userID)
	{
		JSONObject tmp=new JSONObject();
		tmp.put("Type","GET");
		tmp.put("UserID",userID);
		return action(tmp,false);
    }

    public boolean put(String userID,int value)
	{
		JSONObject tmp=new JSONObject();
		tmp.put("Type","PUT");
		tmp.put("UserID",userID);
		tmp.put("Value",value);
		return action(tmp,false)==0;
    }

    public boolean deposit(String userID,int value)
	{
		JSONObject tmp=new JSONObject();
		tmp.put("Type","DEPOSIT");
		tmp.put("UserID",userID);
		tmp.put("Value",value);
		return action(tmp,false)==0;
    }

    public boolean withdraw(String userID,int value)
	{
		JSONObject tmp=new JSONObject();
		tmp.put("Type","WITHDRAW");
		tmp.put("UserID",userID);
		tmp.put("Value",value);
		return action(tmp,false)==0;
    }

    public boolean transfer(String fromID,String toID,int value)
	{
		JSONObject tmp=new JSONObject();
		tmp.put("Type","TRANSFER");
		tmp.put("FromID",fromID);
		tmp.put("ToID",toID);
		tmp.put("Value",value);
		return action(tmp,false)==0;
    }

    public int getLogLength()
	{
        return logLength;
    }

	public int action(JSONObject transaction,boolean isRecoveryTransaction)
	{
		System.out.println("Apply("+transaction+", "+isRecoveryTransaction+")");
		if(!this.recovered&&!isRecoveryTransaction)
			return -1;
		synchronized(balances)
		{
			if(transaction.getString("Type").equals("GET"))
			{
				if(this.recovered)
					return getOrZero(transaction.getString("UserID"));
				else
					return -1;
			}
			else if(transaction.getString("Type").equals("PUT"))
			{
				if(transaction.getInt("Value")<0)
					return -1;
			}
			else if(transaction.getString("Type").equals("DEPOSIT"))
			{
				if(transaction.getInt("Value")<0)
					return -1;
			}
			else if(transaction.getString("Type").equals("WITHDRAW"))
			{
				if(transaction.getInt("Value")<0)
					return -1;
				if(getOrZero(transaction.getString("UserID"))<transaction.getInt("Value"))
					return -1;
			}
			else if(transaction.getString("Type").equals("TRANSFER"))
			{
				if(transaction.getInt("Value")<0)
					return -1;
				if(getOrZero(transaction.getString("FromID"))<transaction.getInt("Value"))
					return -1;
			}

			if(this.recovered)
			{
				try
				{
					JSONObject transactionWithBlockID=new JSONObject(transaction.toString());
					transactionWithBlockID.put("BlockID",blockID);
					Util.writeJsonFile(transientDir+"log_"+logLength+".json",transactionWithBlockID);
					pendingTrans[logLength]=transaction;
					logLength++;
					if(logLength==blockSize)
					{
						JSONObject nextBlock=new JSONObject();
						nextBlock.put("BlockID",blockID);
						nextBlock.put("PrevHash","00000000");
						nextBlock.put("Nonce","00000000");
						JSONArray nextBlockArr=new JSONArray();
						for(int i=0;i<blockSize;i++)
							nextBlockArr.put(pendingTrans[i]);
						nextBlock.put("Transactions",nextBlockArr);
						Util.writeJsonFile(dataDir+blockID+".json",nextBlock);
						blockID++;
						logLength=0;
					}
				}
				catch(IOException e)
				{
					return -1;
				}
			}
			else if(!isRecoveryTransaction)
				return -1;
			if(transaction.getString("Type").equals("PUT"))
				balances.put(transaction.getString("UserID"),transaction.getInt("Value"));
			else if(transaction.getString("Type").equals("DEPOSIT"))
			{
				int tt=getOrZero(transaction.getString("UserID"));
				balances.put(transaction.getString("UserID"),tt+transaction.getInt("Value"));
			}
			else if(transaction.getString("Type").equals("WITHDRAW"))
			{
				int tt=getOrZero(transaction.getString("UserID"));
				balances.put(transaction.getString("UserID"),tt-transaction.getInt("Value"));
			}
			else if(transaction.getString("Type").equals("TRANSFER"))
			{
				int tt1=getOrZero(transaction.getString("FromID"));
				int tt2=getOrZero(transaction.getString("ToID"));
				balances.put(transaction.getString("FromID"),tt1-transaction.getInt("Value"));
				balances.put(transaction.getString("ToID"),tt2+transaction.getInt("Value"));
			}
			return 0;
		}
	}

	public void recover()
	{
		synchronized(balances)
		{
			try
			{
				for(blockID=1;Util.checkFile(dataDir+blockID+".json");blockID++)
				{
					System.out.println("Recovering blockID="+blockID);
					JSONObject tmp=Util.readJsonFile(dataDir+blockID+".json");
					if(tmp.getInt("BlockID")!=blockID)
						break;
					JSONArray tmpArr=(JSONArray)(tmp.get("Transactions"));
					int len=tmpArr.length();
					for(int i=0;i<len;i++)
						action((JSONObject)(tmpArr.get(i)),true);
				}
				for(logLength=0;logLength<blockSize&&Util.checkFile(transientDir+"log_"+logLength+".json");logLength++)
				{
					System.out.println("Recovering blockID="+blockID+" logID="+logLength);
					JSONObject tmp=Util.readJsonFile(transientDir+"log_"+logLength+".json");
					if(tmp.getInt("BlockID")!=blockID)
						break;
					tmp.remove("BlockID");
					action(tmp,true);
					pendingTrans[logLength]=tmp;
				}
				if(logLength==blockSize)
				{
					JSONObject nextBlock=new JSONObject();
					nextBlock.put("BlockID",blockID);
					nextBlock.put("PrevHash","00000000");
					nextBlock.put("Nonce","00000000");
					JSONArray nextBlockArr=new JSONArray();
					for(int i=0;i<blockSize;i++)
						nextBlockArr.put(pendingTrans[i]);
					nextBlock.put("Transactions",nextBlockArr);
					Util.writeJsonFile(dataDir+blockID+".json",nextBlock);
					blockID++;
					logLength=0;
				}
			}
			catch(IOException e)
			{
				System.out.println("IOException at recovery");
				System.exit(-1);
			}
			recovered=true;
		}
	}

}

