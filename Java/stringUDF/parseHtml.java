package votm_udf;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;


public class parseHtml extends EvalFunc<String>
{
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) return null;
		try{
			String str = (String)input.get(0);
			Document doc = Jsoup.parse(str);
			String text_parsed = doc.body().text();
			return text_parsed;
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
