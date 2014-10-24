package stringUDF;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class getNumericOnly extends EvalFunc<String> 
{
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) return null;
		try{
			String str = (String)input.get(0);
			if (str == null) return null;
			String newstr = new String();
			for (int i = 0; i < str.length(); i++) {
				char cTmp = str.charAt(i);
				if (cTmp >= '0' && cTmp <= '9' ) newstr = newstr + cTmp;
			}
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
