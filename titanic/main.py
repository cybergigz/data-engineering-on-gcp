import streamlit as st


st.title("Hello Titanic Survivor Basic ML App")

# data = st.slider("Test", 0, 5, 1)
# print(data)

option = st.(
   "How would you like to be contacted?",
   ("Email", "Home phone", "Mobile phone"),
   index=None,
   placeholder="Select contact method...",
)
st.write('You selected:', option)

if st.button("Predict"):
    # read model
    # predict55
    if True:
        st.subheader(f'This is good 👍')
    else:
        st.subheader(f'This is bad 👎')

    st.write("Results here")