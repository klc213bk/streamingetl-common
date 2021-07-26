package com.transglobe.streamingetl.common.app;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.RandomAccessFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

public class SplitFileApp {

	private static String FILE_NAME = "/home/steven/logs/pcr420669-consumer/pcr420669-consumer.2021-07-22";

	public static void main(String[] args) {

		SplitFileApp app = new SplitFileApp();
		try {
			long t0 = System.currentTimeMillis();
			long lines = app.countLine();
			System.out.println("line" + lines + ",span=" + (System.currentTimeMillis() - t0));

			app.split();


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
		String filename= "/home/steven/logs/pcr420669-consumer/pcr420669-consumer.2021-07-22";
		RandomAccessFile raf = new RandomAccessFile(filename, "r");
		long numSplits = 10; //from user input, extract it from args
		long sourceSize = raf.length();
		System.out.println("sourceSize:" + sourceSize);
		
		long bytesPerSplit = sourceSize/numSplits ;
		long remainingBytes = sourceSize % numSplits;
		
		System.out.println("bytesPerSplit:" + bytesPerSplit+",remainingBytes:" + remainingBytes);

		int maxReadBufferSize = 8 * 1024; //8KB
		for(int destIx=1; destIx <= numSplits; destIx++) {
			BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(filename+".split."+destIx));
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
		}
		if(remainingBytes > 0) {
			BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream("split."+(numSplits+1)));
			readWrite(raf, bw, remainingBytes);
			bw.close();
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
}
