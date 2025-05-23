# Recommender system

Here are the first applications for the graph component in Fink.

## Tag-based analysis

Idea: users tag objects. From these tags, they can search for closest objects in the graph. We start from a few tags (the seed), and as we acquire more tags, the search should become more personnalised (from seed to tree).

Question: 
- how many tags are required to get relevant results?
- how many tags should be available?
- how to handle different tags for the same object?
- can we also automatise some tagging mechanism (i.e. from manual tag assignment to automatic science module)?

## Classification transfer

Goal: compare two classification schemes (how C0 and C1 relates to each other).

## Todo

todo:
- [ ] `recommender` route in the API to get similar objects
- [ ] New HBase table to store obj/tags
- [ ] Add tag button in web portal
- [ ] Add tag as classification mechanism based on tags in graph tools
- [ ] Contact pilot teams to test capabilities


