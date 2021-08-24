package test.suman;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ServerAsFunction {
	
	static class Result{
		
		public final String res;

		 Result(String res) {
			super();
			this.res = res;
		}
		
		
		
	}
	

	public ServerAsFunction() {
		// TODO Auto-generated constructor stub
	}
	
	
	//future combinators
	
		//exceptionally  over partial functions
		
		//flatmap expresses data dependency between  2 async methods
		static  CompletableFuture<Set<Result>> customizedQuery(String user, String origQuery){
			
			return rewrite(user,origQuery)
					.exceptionally((e)->origQuery)
					.thenCompose((rewrittenQuery)->query(rewrittenQuery));
			
		}
	
	
	
	
	
	////////
	
	static CompletableFuture<String> rewrite(String user, String origQuery){
		return !(origQuery.startsWith("failp"))? 
				CompletableFuture.supplyAsync(()->String.format("[customized_query user=%s query=%s]",user,origQuery)): 
					CompletableFuture.failedFuture(new RuntimeException("failp - failed personalization"));
	}
	
	static CompletableFuture<Set<Result>> query(String query){
		//Stream.
		
		System.out.println("running the query !! 11 ");
		
		return collect(Arrays.asList(1,2,3,4,5).stream().map((i)->querySegment(i,query)).collect(Collectors.toList()))
				.thenApply((rs)->rs.stream().flatMap(sets->sets.stream()).collect(Collectors.toSet())) ;
		

	}
	
	static CompletableFuture<Set<Result>> querySegment(Integer segmentId , String query){
		return CompletableFuture.supplyAsync( ()-> Set.of( new Result(String.format("Result segement=%d of query=%s",segmentId , query) )));
	}
	
	static CompletableFuture<Set<Result>> degradedQuery(String query){
		return CompletableFuture.supplyAsync( ()-> Set.of( new Result(String.format("degraded- Result of query=%s",query) )));
	}
	
	
	
	
	
	// collect from a scatter-gather operation
	public static <T> CompletableFuture<List<T>> collect(List<CompletableFuture<T>> tasks){
		//tasks.stream().map(f->new CompletableFuture<>().)
		//CompletableFuture.
		CompletableFuture<Void> allDoneFuture =  CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0] ));
		
		return allDoneFuture.thenApply(v-> 
			tasks.stream()
			     .map(f -> f.join())
			     .collect(Collectors.toList()));
		
	}
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
	//	System.out.println(" run customized query --> "+ customizedQuery("user1" , "query1"));
		System.out.println(" run customized query --> "+ customizedQuery("user1" , "query1").get().stream().map(x->x.res).collect(Collectors.toList()));
//		
		System.out.println(" run customized query - customization fails --> "+ customizedQuery("user1" , "failp_query1").get().stream().map(x->x.res).collect(Collectors.toList()));
//		
	}

}
