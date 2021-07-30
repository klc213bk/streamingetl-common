package com.transglobe.streamingetl.common.app;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

public class SplitFileApp {

	private static String FILE_NAME = "/home/steven/logs/pcr420669-consumer/pcr420669-consumer.log";

	private List<String> splitFileNameList = new ArrayList<>();

	public static void main(String[] args) {

		SplitFileApp app = new SplitFileApp();
		try {
			long t0 = System.currentTimeMillis();
			long lines = app.countLine();
			System.out.println("line" + lines + ",span=" + (System.currentTimeMillis() - t0));

			app.split();

			// list error
			app.listError();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	private long countLine() throws IOException {
		File file = new File(FILE_NAME);
		long lines = 0;
		try (LineNumberReader lnr = new LineNumberReader(new FileReader(file))) {
			while (lnr.readLine() != null);

			lines = lnr.getLineNumber();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lines;

	}
	public void split() throws Exception
	{
		String filename= FILE_NAME;
		RandomAccessFile raf = new RandomAccessFile(filename, "r");
		long numSplits = 100; //from user input, extract it from args
		long sourceSize = raf.length();
		System.out.println("sourceSize:" + sourceSize);

		long bytesPerSplit = sourceSize/numSplits ;
		long remainingBytes = sourceSize % numSplits;

		System.out.println("bytesPerSplit:" + bytesPerSplit+",remainingBytes:" + remainingBytes);

		int maxReadBufferSize = 8 * 1024; //8KB
		String splitFileName = "";
		for(int destIx=1; destIx <= numSplits; destIx++) {
			splitFileName = filename+".split."+destIx;
			BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(splitFileName));
			if(bytesPerSplit > maxReadBufferSize) {
				long numReads = bytesPerSplit/maxReadBufferSize;
				long numRemainingRead = bytesPerSplit % maxReadBufferSize;
				for(int i=0; i<numReads; i++) {
					readWrite(raf, bw, maxReadBufferSize);
				}
				if(numRemainingRead > 0) {
					readWrite(raf, bw, numRemainingRead);
				}
			}else {
				readWrite(raf, bw, bytesPerSplit);
			}
			bw.close();
//			System.out.println("splitFileName="+splitFileName);
			splitFileNameList.add(splitFileName);
		}
		if(remainingBytes > 0) {
			splitFileName = filename+".split."+(numSplits+1);
			BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(splitFileName));
			readWrite(raf, bw, remainingBytes);
			bw.close();

//			System.out.println("splitFileName="+splitFileName);
			splitFileNameList.add(splitFileName);
		}
		raf.close();
	}

	static void readWrite(RandomAccessFile raf, BufferedOutputStream bw, long numBytes) throws IOException {
		byte[] buf = new byte[(int) numBytes];
		int val = raf.read(buf);
		if(val != -1) {
			bw.write(buf);
		}
	}
	private void listError() throws IOException {
		
		String txt = null;
		Pattern pattern = Pattern.compile("error");   

		System.out.println("splitFile size= " + splitFileNameList.size());

		for (String searchFileName : splitFileNameList) {
			Scanner txtscan = new Scanner(new File(searchFileName));
			int line = 0;
			while ((txt = txtscan.findWithinHorizon(pattern,0)) != null)   {
				System.out.println(searchFileName + ",line " + line + " :: " + txt);
				line++;
			}
			if (line == 0) {
				System.out.println(searchFileName + " found no error");
			}
		}
	}
}
