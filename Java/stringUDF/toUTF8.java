package stringUDF;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class toUTF8 extends EvalFunc<String> 
{
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) return null;
		try{
			String str = (String)input.get(0);
			if (str == null) return null;
			byte[] utf8Bytes = str.getBytes("UTF8");
			String newstr = new String(utf8Bytes, "UTF8");
			//newstr.replaceAll("[^\\u0000-\\uFFFF]", "");
			//newstr.replaceAll("[\\uD800-\\uDFFF]","");
			
	        Pattern unicodeOutliers = Pattern.compile("[\\uD800-\\uDFFF]", Pattern.UNICODE_CASE | Pattern.CANON_EQ | Pattern.CASE_INSENSITIVE);
	        Matcher unicodeOutlierMatcher = unicodeOutliers.matcher(newstr);
	        newstr = unicodeOutlierMatcher.replaceAll(" ");
        
			return newstr;
			}catch(Exception e){
				throw new IOException("Caught exception processing input row ", e);
			}
		}
	public Schema outputSchema(Schema input) {
		try {
	        Schema.FieldSchema field_1 = new Schema.FieldSchema("parsed_string", DataType.CHARARRAY);
	        return new Schema(field_1);        
		}catch(Exception e){
			return null;
		}
	}
}
