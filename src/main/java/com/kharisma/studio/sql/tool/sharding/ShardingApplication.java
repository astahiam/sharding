package com.kharisma.studio.sql.tool.sharding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.BufferedWriter;
import java.io.FileWriter;

@SpringBootApplication
public class ShardingApplication implements CommandLineRunner {
	private static Logger LOG = LoggerFactory
			.getLogger(ShardingApplication.class);
	public static void main(String[] args) {
		LOG.info("STARTING THE APPLICATION");
		SpringApplication.run(ShardingApplication.class, args);
		LOG.info("APPLICATION FINISHED");
	}

	@Override
	public void run(String... args) {
		LOG.info("EXECUTING : command line runner");
		//writeSqlFile(selectScript());
		//writeSqlExecuteFile(deleteScript());
		writeSqlRollbackFile(updateScript());
		for (int i = 0; i < args.length; ++i) {
			LOG.info("args[{}]: {}", i, args[i]);
		}
	}

	private void writeSqlFile(String mainScript){
		try {
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("sql.txt"));
			bufferedWriter.write(mainScript);
			bufferedWriter.close();
		} catch (Exception ex){
			LOG.info(ex.getMessage());
		}

	}

	private void writeSqlExecuteFile(String mainScript){
		try {
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("sqlExecute.txt"));
			bufferedWriter.write(mainScript);
			bufferedWriter.close();
		} catch (Exception ex){
			LOG.info(ex.getMessage());
		}

	}

	private void writeSqlRollbackFile(String mainScript){
		try {
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("sqlRollback.txt"));
			bufferedWriter.write(mainScript);
			bufferedWriter.close();
		} catch (Exception ex){
			LOG.info(ex.getMessage());
		}

	}

	private String selectScript(){
		String basicScript1 = "SELECT * FROM accumulate_daily WHERE accumulate_code = 'PAYMENT' and created_time < '20200603';";
		String basicScript2 = "SELECT * FROM accumulate_weekly WHERE accumulate_code = 'PAYMENT' and created_time < '20200603';";
		String basicScript3 = "SELECT * FROM accumulate_monthly WHERE accumulate_code = 'PAYMENT' and created_time < '20200603';";
		String result = splitPhysicalTable(basicScript1,3) +splitPhysicalTable(basicScript2,3) +splitPhysicalTable(basicScript3,3);
		return result;
	}

	private String deleteScript(){
		String basicScript1 = "DELETE FROM accumulate_daily WHERE accumulate_code = 'PAYMENT' and created_time < '20200603';";
		String basicScript2 = "DELETE FROM accumulate_weekly WHERE accumulate_code = 'PAYMENT' and created_time < '20200603';";
		String basicScript3 = "DELETE FROM accumulate_monthly WHERE accumulate_code = 'PAYMENT' and created_time < '20200603';";
		String basicScript4 = "DELETE FROM unique WHERE created_time < '20200603';";

		String result = splitPhysicalTable(basicScript1, 2)  + splitPhysicalTable(basicScript2, 2) +  splitPhysicalTable(basicScript3, 2) + splitPhysicalTable(basicScript4, 2) ;
		return result;
	}

	private String updateScript() {
		//String basicScript1 = "UPDATE accumulate_queue set processed = 'T' WHERE queue_id in (SELECT row_key FROM idt_unique );";
		String basicScript1 = "DELETE FROM accumulate_queue WHERE processed = 'F';";
		return splitPhysicalTable(basicScript1, 2);
		//return splitPhysicalTable2(basicScript1, 1, 12);
	}

	private String splitPhysicalTable(String basicScript1, int tableNamePosition){
		String[] arrayScript = basicScript1.split(" ");
		String tableName = arrayScript[tableNamePosition];
		String newTableName, belowTen, upperTen;
		String[] newBasicScript1 = new String[100];
		int counter = 0;
		for (int i= 0; i<100; i++){
			belowTen = tableName + "_00" + counter;
			upperTen = tableName + "_0" + counter;
			newTableName = (counter < 10) ? belowTen : upperTen;
			newBasicScript1[i] = "";
			for( int j=0 ; j < arrayScript.length;j++){
				String item = (j == tableNamePosition) ? newTableName : arrayScript[j];
				newBasicScript1[i] += item + " ";
			}
			counter++;
		}
		String result = "";
		for (int k=0; k < 100; k++){
			result += newBasicScript1[k];
		}
		return result;
	}

	private String splitPhysicalTable2(String basicScript1, int tableNamePosition1, int tableNamePosition2){
		String[] arrayScript = basicScript1.split(" ");
		String tableName1 = arrayScript[tableNamePosition1];
		String tableName2 = arrayScript[tableNamePosition2];
		String newTableName1, newTableName2, belowTen, upperTen, item;
		String[] newBasicScript1 = new String[100];
		int counter = 0;
		for (int i= 0; i<100; i++){
			belowTen = tableName1 + "_00" + counter;
			upperTen = tableName1 + "_0" + counter;
			newTableName1 = (counter < 10) ? belowTen : upperTen;
			belowTen = tableName2 + "_00" + counter;
			upperTen = tableName2 + "_0" + counter;
			newTableName2 = (counter < 10) ? belowTen : upperTen;
			newBasicScript1[i] = "";
			for( int j=0 ; j < arrayScript.length;j++){
				if (j == tableNamePosition1){
					item = newTableName1;
				} else if (j == tableNamePosition2) {
					item = newTableName2;
				} else {
					item = arrayScript[j];
				}
				newBasicScript1[i] += item + " ";
			}
			counter++;
		}
		String result = "";
		for (int k=0; k < 100; k++){
			result += newBasicScript1[k];
		}
		return result;
	}
}
