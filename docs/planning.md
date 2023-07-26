# Planning

Main data structure, rooted at planning item:

``` json
{
    "uuid": "b622d028-b423-4a7e-92d9-6fe07b4b80be",
    "created": "2023-07-24T08:00:36+0200",
    "modified": "2023-07-24T08:01:00+0200",
    "title": "Sommarporträttet: radioprofilen",
    "status": "usable",
    "public": true,
    "publish": "[2023-07-27T15:00:00+0200]",
    "publish_slot": 12,
    "urgency": 2,
    "data": {
        "meta": [
            {"type":"tt/slugline", "value": "sommar"},
            {
                "type": "core/description",
                "role": "public",
                "data": {
                    "text": "Hennes jobb är att vara \"en personlighet på plats\" – klockan 06.00. Beskrivs som en av landets roligaste, hon sänder direkt i P3 och vill gärna vara på gränsen. Vi har intervjuat henne.",
                }
            },
            {
                "type": "core/description",
                "role": "internal",
                "data": {
                    "text": "Vad: Vi träffar radioprofilen (och numera även tv-profilen) för en intervju i vår serie sommarporträtt.\nVar: Lilla caféet på Söder, Ringvägen 131.\nKontakt till profilen: 070/XXXXXYY.",
                }
            }
        ],
        "links": [
            {
                "title": "Kultur och nöje",
                "rel": "sector",
                "value": "KLT"
            }
        ]
    },
    "units": ["core://unit/redaktionen"],
    "coverage": "[d1338a6d-e0a7-4413-be7e-7d6be74d2c55]",
    "deliverables": [
        {"uuid":"7c5f41f6-c515-4cf4-aab9-42c660616f61"},
        {"uuid":"e1611bc7-c663-4428-8111-c90717483670"}
    ],
    "assignments": [
        {
            "uuid": "2e2b8f33-5948-4add-97fe-5f7dab63f065",
            "starts": "2023-07-25T00:00:00+0200",
            "ends": "2023-07-25T23:59:59+0200",
            "status": "done",
            "full_day": true,
            "kind": ["text"],
            "assignees": [
                {
                    "uuid": "84ec9855-32b5-451c-a0e4-c91968fd5328",
                    "name": "Erika",
                    "role": "primary"
                },
                {
                    "uuid": "74fd578d-6a7b-4e83-ab4f-c0006c268838",
                    "name": "Sara",
                    "role": "fallback"
                },
                {
                    "uuid": "9c26ee5e-4391-45dc-996d-06cd7f37bd93",
                    "name": "Sara",
                    "role": "fallback"
                }
            ]
        },
        {
            "uuid": "c85caaa0-8498-4666-a999-be22623d0261",
            "starts": "2023-07-25T11:30:00+0200",
            "ends": "2023-07-25T13:30:00+0200",
            "status": "done",
            "full_day": false,
            "kind": ["photo"],
            "assignees": [
                {
                    "uuid": "94d0e6be-38c4-4485-ae19-1d6f779648dc",
                    "name": "Anders",
                    "role": "primary"
                }
            ]
        }
    ]
}
```

The internal description partly comes from the photographer's assignment. On the text side, we undermodel and do not specify the time (or place) of the interview. Do journalists usually check such details in the photographers' assignments instead? How does it work in practice?

Three journalists have also been assigned to the task. In the photographer's assignment, it was noted that it was likely Erika who would do the interview. Is there a convention that the first assignee on the list is considered the primary? Trying to model with "role" here.

I'm not a big fan of storing contact information in free text within the description. Even though GDPR doesn't apply to our journalistic work it would be better information hygiene to integrate it with contact information data.

There's also un-modelled location data in the assignment that could be formalised in the planning tool and data. Theres also information in the assignment title in [planning/assignment_text.xml] like "Fullängdstext, fakta." that probably should be structured data, I'm unsure about at which level it would make sense though. Planning item, or assignment? Location is related to where the assignment should be carried out, information about the characteristics of the content that will/should be delivered most likely belongs on the planning item.

In my proposed model assignments and deliverables are siblings in the planning item. That's because I see more of a 1-1 mapping between planning items and a... deliverable unit, in lack of a better name. So the deliverables should be the article and associated "assets" like images, factboxes, audio, et.c that should be published together as a whole.

This represent three major changes:

1. the previous use of planning items to represent ongoing long term coverage of a topic (Russian war in Ukraine, long sport events et.c.) should be replaced by the new planning coverage entity:

``` json
{
    "uuid": "2eb31200-1375-4f59-9b5b-843baf37892a",
    "created": "2023-07-24T11:51:55+0200",
    "modified": "2023-07-24T11:52:01+0200",
    "title": "Löpande rapportering om kriget i Ukraina",
    "description": "",
    "status": "usable",
    "public": true,
    "starts": "2022-01-01",
    "data": {
        "links": [
            {
                "uuid": "8ea26b1e-e5b4-415a-b99c-7e8571c30662",
                "type": "core/story",
                "title": "Kriget i Ukraina",
                "rel": "story"
            }
        ]
    },
    "units": ["core://unit/redaktionen"]
}
```

2. The planning item controls scheduled publishing. This gets rid of the the double book-keeping with a withheld status on and publication timestamp on the article, and a publication timestamp on the planning item. I think we have two options re. what version of the article to publish: either we publish the currently withheld version of the article (that has the bonus of making it directly apparent on the article that it's being withheld for publishing, and which version will be published), or we set a document version on the planning item. I think that the first option is the easier one to work with, and if the need arises we could extend the status model with the ability to also lock to either a document version or status ID, but I don't think that there'll be a need to fuss around with versions on that detail level, it probably just leads to confusion. 

3. There's no longer a link between an assignment and the deliverable. The planning item has links to deliverables, and assignees have roles/jobs to fulfil in the context of the planning item. I think that this makes more sense, and I don't see any real loss of capability.

## Queryability

The entry point for queries are going to be planning items and planning coverage. Planning data is primarily going to be filtered by date, that should narrow the candidate set down enough for most other filtering to be done without indexes without ill effects. Might even be a good idea to turn to client based filtering to cut down on client-server back and forth after the data set has been downloaded.

Being flexible in querying and composing data for views ([this article](https://buttondown.email/hillelwayne/archive/queryability-and-the-sublime-mediocrity-of-sql/) springs to mind, and I think "sublime mediocrity" is a worthy design goal to aspire to) will cut down on the active data distortion that we see today with repetition and inconsistencies being introduced to make information turn up in specific views or applications.

## Mutation and live updates

The object structure representations of the planning data above would ideally be the way that our user facing applications interact with the data through a Y.js document. That means that we will need a way to diff the current state against db state and perform the necessary mutations to the DB. I think that we'll also need to store the Y.js state in the database.

A big difference to the document data is that the Y.js document should be the only way to update the planning data. We can of course expose API:s for performing discrete operations against the state for more light-weight integrations, but those would be applied against the Y.js doc followed by a forced diff & flush to db.

When it comes to live updates it's a given that it should be driven by Y.js while in a detail or editing view. For the list overview it's less clear cut, is it too expensive to fetch all planning items for a week and then instantiate and subscribe to them all as y.js documents? Can we multiplex many documents on a single websocket? Otherwise the fallback should probably be an event stream that f.ex. just emits updated object IDs and their date range, and then lets the client decide what to do with it, though that might actually lead to more data being shuffled due to loss of delta format. Another option is to poll at regular intervals, which might be good enough, it would also let us use SQL to get just the data that's needed for the view.

## What is a status?

Status is currently very overloaded in NRP, especially for us who not only deal with the workflow status (something we: might do, will do, or have decided not to do) but also whether we want to publish it to our customers.

I would suggest four statuses for planning items:

* draft
* tentative
* planned
* cancelled

This would work together with `public` to form the status of a planning item. A "draft" must never be published to customers regardless of `public`, public would in that case communicate that this is a planning item we **intend** to make public. The difference between "tentative" and "planned" is one of confidence. "cancelled" should always be used when we decide to not go forward with planned delivery that went beyond the draft stage. Drafts can just be deleted. 

It could be argued that these statuses conflate workflow status and confidence level, and while that's true I think that confidence level fits nicely on the same axis that the workflow status moves along:

* draft: missing information - early days not sure yet
* tentative: good enough to publish - decent probability that we'll do this
* planned: good enough to publish - to the best of our knowledge we'll do this
* cancelled: was good enough, but got cancelled - we don't plan to do this anymore

This model would become unfit if we want to start modelling confidence with more granularity, say: unlikely, tentative, probable, for-sure, come-hell-or-high-water. Then it would make more sense to split out confidence to its own value. Though I think that even if this gives us a clear way to communicate confidence in, say, a 0-100% range, it becomes less valuable as a signal and misreprents our ability to measure the probability.
