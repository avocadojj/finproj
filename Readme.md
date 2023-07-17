To Do :
1. Script untuk memberi ID dan membagi tiap date.
2.



==================================================
Why need streaming ?
The main reason is that this kind of data is usually updated on a daily, weekly or monthly basis, rather than in real-time. Each entry is a distinct event that doesn't depend on previous entries. Furthermore, the operations that you might want to perform on this dataset such as aggregations, analytics, machine learning model training etc., are typically done in a batch manner.

However, if you're looking for real-time insights or if the data changes frequently (e.g., new data comes every few seconds or minutes), then streaming could be a better option. This could be the case if, for instance, the 'duration' column represented ongoing time spent on a phone call and you wanted to analyze this in real time.

