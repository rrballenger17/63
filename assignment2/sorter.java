import java.util.*;
import java.util.regex.*;
import java.io.*;

public class sorter{

    public static HashMap<Integer, ArrayList<String>> map; 
    public static Set<String> stopsWordsSet;
    public static int max;

    // writeIt searches for each possible count starting at the max. It gets
    // all the lines with that particular count from the map. It sorts them alphabetically. 
    // Finally it outputs the line and the count.
    public static void writeIt(){
    	for(int x=max; x>=0; x--){
    		ArrayList<String> list = map.get(x);

    		if(list!= null){
    			Comparator<String> comparator = new Comparator<String>(){
    				@Override
    				public int compare(final String one, final String two){
        				return one.toLowerCase().compareTo(two.toLowerCase());
    				}
				};

    			Collections.sort(list, comparator);

    			for(int a=0; a<list.size(); a++){
                    Pattern p = Pattern.compile("\\S+\\s");
                    Matcher m = p.matcher(list.get(a));
                    if (m.find()){
                        //System.out.println(Integer.parseInt(m.group().replaceAll("\\s+","")));
                            String word = m.group().replaceAll("\\s+","");

                            if(!stopsWordsSet.contains(word.toLowerCase())){
                                System.out.println(list.get(a));
                            }
                    }
    			}
    		}
    	}
    }



    // mapIt adds the line to the map's arraylist for that count. If no lines have
    // the count yet, a new arraylist is created and added to the map.
	public static void mapIt(int i, String s){

		if(map.containsKey(i)){
			ArrayList<String> list = map.get(i);
			list.add(s);
			map.put(i, list);
		}else{
			ArrayList<String> list = new ArrayList<String>();
			list.add(s);
			map.put(i, list);
		}

	}


    // function reads the part-r-00000 file and adds the count and the entire line
    // to the map named map.
   	public static void function(){
   		try(BufferedReader br = new BufferedReader(new FileReader("part-r-00000"))) {
    	for(String line; (line = br.readLine()) != null; ) {
        	
    		Pattern p = Pattern.compile("\\s[0-9]+");
			Matcher m = p.matcher(line);
			if (m.find()){
				//System.out.println(Integer.parseInt(m.group().replaceAll("\\s+","")));

				int count = Integer.parseInt(m.group().replaceAll("\\s+",""));

				if(count > max){
					max = count;
				}
				mapIt(count, line);
			}
    	}
		}catch(Exception e){
		}
   	}

    // loadStopWords gets the stop words from stopwords.txt and adds them to Set<String>stopWordsSet
    public static void loadStopwords(){

        stopsWordsSet = new HashSet<String>();

        try(BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"))) {
        for(String line; (line = br.readLine()) != null; ) {
                String s = line.replaceAll("\\s+","");
                stopsWordsSet.add(s);
        }
        }catch(Exception e){

        }
    }


    public static void main(String []args) {

    	map = new HashMap<Integer, ArrayList<String>>();

    	max = 0;

    	System.out.println("Hello World"); // prints Hello World

        loadStopwords();

    	function();

    	writeIt();
    }




} 





