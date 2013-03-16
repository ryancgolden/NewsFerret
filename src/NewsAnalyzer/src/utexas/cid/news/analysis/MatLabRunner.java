package utexas.cid.news.analysis;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import utexas.cid.news.Constants;

public class MatLabRunner {
	
	public static void main(String[] args) throws IOException, InterruptedException {
		MatLabRunner.runMatlabSteps();
	}
	
	/**
	 * Run Matlab "main_id" method
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void runMatlabSteps() throws IOException, InterruptedException {

		// MATLABCONTROL didn't work for me on Ubuntu12.04, 64-bit, MATLAB 2009a
		//
		// throws MatlabConnectionException, MatlabInvocationException {
		// // TODO Auto-generated method stub
		// MatlabProxyFactoryOptions options = new
		// MatlabProxyFactoryOptions.Builder()
		// .setMatlabStartingDirectory(new File("/win/UT/github/ID/src/matlab"))
		// .setLogFile("/win/UT/github/ID/src/matlab/matlab.out")
		// .setMatlabLocation("/usr/local/mathworks/bin/matlab")
		// .setHidden(true)
		// .build();
		// MatlabProxyFactory factory = new MatlabProxyFactory(options);
		// MatlabProxy proxy = factory.getProxy();
		// proxy.eval("main_id");
		// proxy.disconnect();

		Process p = null;
		try {
			p = Runtime
					.getRuntime()
					.exec(Constants.MATLAB_CMD);
		} catch (IOException e) {
			System.err.println("Error on exec() method");
			e.printStackTrace();
		}
	    copy(p.getInputStream(), System.out);
	    copy(p.getErrorStream(), System.err);  // can ignore shopt error if on Linux
	    p.waitFor();	

	}

	static void copy(InputStream in, OutputStream out) throws IOException {
		while (true) {
			int c = in.read();
			if (c == -1)
				break;
			out.write((char) c);
		}
	}

}
